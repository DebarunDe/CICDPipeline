from aws_cdk import (
    Stack,
    SecretValue,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as cpactions,
    aws_codebuild as codebuild,
    aws_secretsmanager as secretsmanager,
    aws_glue as glue,
    aws_s3_assets as s3_assets,
    aws_iam as iam,
    aws_codebuild as codebuild,
)
from constructs import Construct


class CICDPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Reference GitHub token from Secrets Manager
        github_token = secretsmanager.Secret.from_secret_name_v2(
            self, "GitHubToken", "GITHUB_TOKEN"
        ).secret_value_from_json("GITHUB_TOKEN")

        # Define the pipeline
        pipeline = codepipeline.Pipeline(self, "ETLPipeline",
            pipeline_name="GlueETL-CICD-Pipeline"
        )

        #Define roles
        #pipeline role
        pipeline_role = pipeline.role
        
        # Create an IAM role for the Glue job
        glue_role = iam.Role(self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )
        
        # Create an IAM role for the deploy
        deploy_role = iam.Role(
            self,
            "DeployRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("codebuild.amazonaws.com"),  # CodeBuild service
                iam.ArnPrincipal(pipeline.role.role_arn)          # CodePipeline pipeline role
            ),
            description="Role assumed by CodeBuild project to deploy CDK stack",
        )
        deploy_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")
        )

        # Output artifact from the source stage
        source_output = codepipeline.Artifact()

        # Define the source stage (GitHub repo)
        source_action = cpactions.GitHubSourceAction(
            action_name="GitHub_Source",
            owner="DebarunDe",  # <-- CHANGE THIS
            repo="CICDPipeline",         # <-- CHANGE THIS
            oauth_token=github_token,
            output=source_output,
            branch="main",
            trigger=cpactions.GitHubTrigger.WEBHOOK,
        )

        # Define CodeBuild project
        build_project = codebuild.PipelineProject(
            self, "BuildProject",
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_6_0
            )
        )

        build_output = codepipeline.Artifact()

        build_action = cpactions.CodeBuildAction(
            action_name="Build",
            project=build_project,
            input=source_output,
            outputs=[build_output]
        )
        
        deploy_project = codebuild.Project(
            self,
            "DeployProject",
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                compute_type=codebuild.ComputeType.SMALL,
                environment_variables={
                    "CDK_DEFAULT_ACCOUNT": codebuild.BuildEnvironmentVariable(value=self.account),
                    "CDK_DEFAULT_REGION": codebuild.BuildEnvironmentVariable(value=self.region),
                },
            ),
            build_spec=codebuild.BuildSpec.from_object({
                "version": "0.2",
                "phases": {
                    "install": {
                        "runtime-versions": {
                            "python": "3.11"
                        },
                        "commands": [
                            "npm install -g aws-cdk",
                            "pip install -r requirements.txt"
                        ]
                    },
                    "build": {
                        "commands": [
                            "cdk deploy --require-approval never"
                        ]
                    }
                }
            }),
            role=deploy_role,
        )    

        pipeline.add_stage(
            stage_name="Source",
            actions=[source_action]
        )

        pipeline.add_stage(
            stage_name="Build",
            actions=[build_action]
        )
        
        pipeline.add_stage(
            stage_name="Deploy",
            actions=[
                cpactions.CodeBuildAction(
                    action_name="CDK_Deploy",
                    input=build_output,  # reuse output from build stage
                    project=deploy_project,
                    role=deploy_role,    # optional: create a new IAM role for this
                    run_order=1,
                )
            ]
        )
        
        # Upload the ETL script as an S3 asset
        etl_asset = s3_assets.Asset(self, "ETLScriptAsset",
            path="etl/sample_job.py"
        )
        
        #grant read permissions to glue role
        etl_asset.grant_read(glue_role)

        # Define the Glue Job
        glue.CfnJob(self, "GlueETLJob",
            name="SampleETLJob",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=etl_asset.s3_object_url
            ),
            glue_version="4.0",
            number_of_workers=2,
            worker_type="G.1X",
            default_arguments={
                "--job-language": "python",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true"
            }
        )
