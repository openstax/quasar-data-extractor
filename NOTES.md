# SCRATCH PAD for notes during implementation

Friday, Sept 8, 2023

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

Monday, Sept 11, 2023

Took a little tweaking to make the demo (from
https://github.com/aws-samples/aws-batch-processing-job-repo) work, mostly
correcting for building an amd64 docker image on an arm (Apple M1pro) platform,
and waiting for the stack to deploy before trying to use it.

(Went ahead and submitted a PR to help improve the demo for others)

Overall, seems like a good strategy, discussed w/ Nathan, will probably define
a json format for a "request for dataset", triggered by storing said file to a
specific S3 bucket. May include a callback URL that the extractor script would
call when the extraction is complete.
  
https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/best-practices.html

Digging into how to have cloud formation templates cross-reference between
different repos, since I want to keep the data extractor code here. Starting
with this, may revert to moving the stack info all over to quasar-deployment
(and maybe move the connector scripts stuff here?)  Seems possible, given that
CF values must be unique per region/account, so from the same account w/ the
same permissions, should be able to look them up, even if they're defined
elsewhere (I think) as long as the stack has been deployed that exports them.
Will test w/ some values.

The thing is that the rest of the Quasar deployment stack is not plain vanilla
CF templates, but built to be called by the ruby-based deployment code
originally copied from unified-deployment. Compared to other examples I've
looked at, they depend on a lot more passed-in parameters than is considered
"best practices" for AWS CF templates. THis is a consequence of them actually
being mapped to ruby classes, with all the parameters coming from class
attribute values. Since these get inherited, etc, it's slightly easier to
handle there. I'll have to see how that impacts use from "outside" the repo
that contains both.  However, the extractor code does not need access to _any_
of the typical CF-stack things (like VPC, or specific security groups etc.) All
it really needs is access to the S3 bucket. Arguably, the extractor bits
(fargate config, cloudwatch trigger, lambda, etc) should _not_ have direct
access to the rest of stack, for security containment: best way to avoid having
a coding issue expose PPI is to not only have checks in the code, but limit
permissions from outside.

Tuesday, Sept 12, 2023

Fleshed out the actual access api (json edition) with examples for the 
data set request and results documents. (c.f. REQUEST_API.md) and
https://openstax.atlassian.net/wiki/spaces/KA/pages/2314534913/Research+Data+Access+API+-+details


Dug into what it would take to have separate CF templates that use values from
separate repos: doesn't look like a best practices sort of thing at all  - if
it's doable, it's hard. And the only win is maybe better security (as described above).
So, have decided to stand up a new stack inside the existing quasar-deployment
repo and CF collection, instead. And need to start with that, rather than getting 
the data script working first, since the environment it will run in is critical.


Wednesday, Sept 13, 2023

Task and card breakdown work in the a.m. Had planning meeting re: rest of Q3

Got batch demo actually deploying as part of quasar-deployment, though it's still doing its
"read a csv and put it in a dynamodb" thing, so is not integrated beyond being deployed by the
aws ruby wrapper code.

Thursday, Sept 14, 2023

Lots and lots of CF work: further intergrated extractor stack w/ the rest of quasar deployment
Figured out the docker image creation and upload: convert to using test_load.py as payload
update lambda trigger to invoke proper function. Bam! It works!

Checked w/ Dante re: codebuilder. He suggested bothering to set it up only if the image will
change fairly frequently.  Looking at it a bit more, I'm concerned about auto-deploying to
production, accidently. Will stick w/ manual build/deploy for now (similar to other Quasar stacks)

Friday, Sept 15, 2023

Let's write an actual data extractor script! 
Learning all the details on S3 notifications, lambda execution, and how events are triggered and
passed.

Decision time: workflow, one bucket or two? I'm tempted to make it one S3 bucket: you write your
request in a file name '/some/path/my_request.json' (anything that ends in request.json) and the
results are written to '/some/path/event_data_subset_<jobId>  (or maybe timestamp: event_data_2023_09_20)

That way the enclave only needs to deal with one bucket. It does mean that the data analysis code
will have write privs on the data, but I think that's actually probably correct. It's a copy, anyway.

 
for Monday:
 - pass in the actual event data bucket
 - deal w/ the top-level prefix (the env name) that exists in there
 - writing data
 - threads

Monday, Sept 18, 2023

Completed up to writing data. Works with small sample request (1 event, 100 users, 2 days) Have not yet
measured scale, nor attempted threading in this context. Also needs more error handling, and the callback
reporting is not yet implmented. But it works!

