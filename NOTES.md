# SCRATCH PAD for notes during implementation


Seems using AWS Batch and Farget w/ a docker image is the best first cut see:
https://docs.aws.amazon.com/batch/latest/userguide/what-is-batch.html

https://aws.amazon.com/blogs/developer/aws-batch-application-orchestration-using-aws-fargate/

Hmm, idea: instead of sending all the extraction critera via job configuration, how about have
an S3 bucket (perhaps the one that the data will eventually land in?) and write the required
info (json, probably) to that bucket. Use cloudwatch to trigger a lambda when the appropriate
file is written, launching the Batch to process. This would allow (potentially) to error check
requested data extract before launching the extractor (though that should rpobably be in there, as well)

N.B. Check w/ Dante next week re: codebuilder for building images for AWS

Hmm, may need to not use fargate if performance is not sufficent: doesn't look like I can specify
the image type, just home many cpus are there. I found specc'ing the network bandwidth seemed to be
necessary as well.

Messing w/ CF templates to set up an example that works this way.
