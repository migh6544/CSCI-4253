# Update the package list and install SDK
sudo apt-get update
# sudo apt-get install -y python3 python3-pip git google-cloud-sdk
sudo apt-get install -y python3 python3-pip
pip3 install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

sudo -i #needed for permision issues
mkdir -p /srv
cd /srv
#get files from metadata
curl http://metadata/computeMetadata/v1/instance/attributes/vm2-startup-script -H "Metadata-Flavor: Google" > vm2-startup-script.sh
curl http://metadata/computeMetadata/v1/instance/attributes/service-credentials -H "Metadata-Flavor: Google" > service-credentials.json
curl http://metadata/computeMetadata/v1/instance/attributes/part1 -H "Metadata-Flavor: Google" > part1.py
export GOOGLE_CLOUD_PROJECT= $(curl http://metadata/computeMetadata/v1/instance/attributes/project -H "Metadata-Flavor: Google")

#launch second vm
python3 ./part1.py csci-4253-359820