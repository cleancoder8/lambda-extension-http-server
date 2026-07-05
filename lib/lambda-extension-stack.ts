import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { type Construct } from 'constructs';
import path from 'node:path';

export class LambdaExtensionStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const bucket = new s3.Bucket(this, 'PayloadBucket', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      enforceSSL: true,
    });

    const extensionLayer = new lambda.LayerVersion(this, 'ExtensionLayer', {
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', 'dist', 'extension'),
      ),
      description: 'HTTP server extension for async S3 uploads',
      compatibleRuntimes: [
        lambda.Runtime.NODEJS_18_X,
        lambda.Runtime.NODEJS_20_X,
      ],
      compatibleArchitectures: [lambda.Architecture.ARM_64, lambda.Architecture.X86_64],
    });

    const fn = new NodejsFunction(this, 'Function', {
      runtime: lambda.Runtime.NODEJS_20_X,
      architecture: lambda.Architecture.ARM_64,
      entry: path.join(__dirname, '..', 'src', 'lambda', 'index.ts'),
      handler: 'handler',
      layers: [extensionLayer],
      environment: {
        BUCKET_NAME: bucket.bucketName,
        EXTENSION_PORT: '4000',
      },
      timeout: cdk.Duration.seconds(30),
      memorySize: 512,
      bundling: {
        externalModules: ['@aws-sdk/*'],
      },
    });

    bucket.grantReadWrite(fn);

    new cdk.CfnOutput(this, 'LambdaFunctionName', {
      value: fn.functionName,
      description: 'Lambda function name',
    });

    new cdk.CfnOutput(this, 'BucketName', {
      value: bucket.bucketName,
      description: 'S3 bucket for uploaded payloads',
    });
  }
}
