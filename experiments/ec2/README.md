
To get all the private IPs run the following commmand. You might have to change what the filter is and also before running it you might have to configure the aws region.

```sh
aws ec2 describe-instances --output text --query "Reservations[*].Instances[*].[InstanceType, State.Name, PrivateIpAddress]" --filter Name="instance-type",Values="m6g.medium"
```

If the output seems reasonable you can run this:

```sh
aws ec2 describe-instances --output text --query "Reservations[*].Instances[*].[PrivateIpAddress]" --filter Name="instance-type",Values="m6g.medium"
```

