# mandrill-to-s3
Pipes inboud e-mail from Mandrill webhook to S3 storage


## Setup

1. Create some AWS credentials with upload permissions for S3

1. Press this button to create a new Heroku app:

    <a href="https://heroku.com/deploy" target="_blank">
        <img src="https://www.herokucdn.com/deploy/button.png" alt="Deploy">
    </a>

1. Add your AWS credentials

1. Add name and host for the S3 bucket you whish to upload to. Regions are listed here `http://docs.aws.amazon.com/general/latest/gr/rande.html`

1. Deploy the app.

1. After deployment, click **"View it"** to open the new app and copy the hook url.

1. Go to `https://mandrillapp.com/inbound` click `routes` for the domain which you want to forward from and input the hook url

