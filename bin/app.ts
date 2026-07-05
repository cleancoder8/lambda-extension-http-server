#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { LambdaExtensionStack } from '../lib/lambda-extension-stack';

const app = new cdk.App();

new LambdaExtensionStack(app, 'LambdaExtensionHttpServerStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
