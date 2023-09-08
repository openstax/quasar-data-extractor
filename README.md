# quasar-data-extractor
code for extracting event capture data, subset by various criteria

Will utilize AWS Batch, so this repo is mostly for developing the code and docker
config to create a docker image to run via that service


  [x] 1. Proof of concept code to read from S3 bucket at scale. Timing tests. [complete]
  [ ] 2. dockerfile infrastructure
  [ ] 3. tweak script to be minimally functional, for implementation testing (small set of data)
  [ ] 4. AWS Batch job description (this may more properly live in quasar-deploy, will decide)
  [ ] 5. modify script to add necessary additional functionality

