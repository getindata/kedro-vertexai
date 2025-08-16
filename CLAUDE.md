# Kedro-VertexAI Project Instructions

## Environment Configuration
- Environment settings in: `dev-utils/local.env`

## Debugging Failed Vertex AI Pipelines

### Method to obtain error logs:

1. **Get environment settings from `dev-utils/local.env`**

2. **Find pipeline errors**:
   ```bash
   gcloud logging read "PIPELINE_NAME_OR_ID AND severity=ERROR" \
     --project=gid-labs-mlops-sandbox --limit=10 \
     --format="value(timestamp,jsonPayload,textPayload)" --order="desc"
   ```

3. **Extract job IDs from error messages** (look for `job_id` in error messages)

4. **Get detailed container logs from specific failed jobs**:
   ```bash
   gcloud logging read "resource.type=ml_job AND resource.labels.job_id=JOB_ID" \
     --project=gid-labs-mlops-sandbox --format=json --limit=500 | \
     jq -r '.[] | select(.jsonPayload.message) | .jsonPayload.message' | \
     grep -E "(Error|Exception|Failed|Traceback)" 
   ```

5. **Get full error context**:
   ```bash
   gcloud logging read "resource.type=ml_job AND resource.labels.job_id=JOB_ID" \
     --project=gid-labs-mlops-sandbox --format=json --limit=500 | \
     jq -r '.[] | select(.jsonPayload.message) | .jsonPayload.message' | \
     grep -A20 -B5 "ERROR_KEYWORD"
   ```
   
### Method to obtain pipeline definition: 
1. Execute `kedro vertexai compile`, the pipeline output is saved to pipeline.yml file 