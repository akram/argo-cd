package testdata

import _ "embed"

var (
	//go:embed live_deployment_with_managed_replica.yaml
	LiveDeploymentWithManagedReplicaYaml string

	//go:embed desired_deployment.yaml
	DesiredDeploymentYaml string

	//go:embed live_validating_webhook.yaml
	LiveValidatingWebhookYaml string

	//go:embed live_unmanaged_validating_webhook.yaml
	LiveUnmanagedValidatingWebhookYaml string

	//go:embed desired_validating_webhook.yaml
	DesiredValidatingWebhookYaml string

	//go:embed desired_validating_webhook_nil_cabundle.yaml
	DesiredValidatingWebhookNilCabundleYaml string
)
