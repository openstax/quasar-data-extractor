# quasar-data-extractor
code for extracting event capture data, subset by various criteria

Will utilize AWS Batch, so this repo is mostly for developing the code and docker
config to create a docker image to run via that service


  [x] 1. Proof of concept code to read from S3 bucket at scale. Timing tests. [complete]
  [ ] 2. deployment infrastructure
  [ ]    a. CF templates - integrate w/ quasar-deployment first
  [ ]    b. dockerfile  - also in quasar-deployment, since building images seems to live there?
  [ ] 3. tweak script to be minimally functional, for implementation testing (small set of data)
  [ ] 4. modify script to read json request files, as per design
  [ ] 5. modify script to add necessary additional filtering and writing functionality.
