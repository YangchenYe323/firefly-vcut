# firefly-vcut

A collection of serverless functions, and crons for populating song occurrence data used by [firefly](https://github.com/YangchenYe323/firefly). Use it to construct vtuber calendars and know which songs are played at which point in each live.

## Deployment

The functions are hosted on [modal](https://modal.com/). Set the following environment varaibles in `.env` file under project root:

```
DATABASE_URL=<firefly postrges URL>
R2_ENDPOINT=<cloudflare r2 endpoint>
R2_BUCKET=<cloudflare r2 bucket name for hosting live transcript and audio archive>
AWS_ACCESS_KEY_ID=<cloudflare r2 access key ID>
AWS_SECRET_ACCESS_KEY=<cloudflare r2 secret access key>
BILI_CRED_SESSDATA=<bilibili login credential for video streaming and other API calls>
```

Then run
```Bash
# Deploy the firefly cron to modal
modal deploy -m firefly_vcut.modal.cron --name firefly-vcut-cron
```
