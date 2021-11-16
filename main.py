import boto3
import os
from tqdm import tqdm
from shutil import make_archive

def confirm(prompt):
    answer = ""
    while answer not in ["y", "n"]:
        answer = input(prompt).lower()
    return answer == "y"

#Set AWS cli config
def aws_login():
    """Prompts user for aws credentials and default config
    via the aws cli 

    Returns:
        boolean: status of login
    """
    if(not os.path.exists('./aws-config/credentials')):
        while not os.path.exists('./aws-config/credentials'):
            print('\n|-----⚙️  AWS Configuration⚙️-----|\n')
            os.system('aws configure')
        return True
    print('\nCongrats you have pre-configured credentials 🔥👏\n')
    os.system('aws configure list')
    if confirm('\nWould you like to logout and re-setup your configuration? [Y/N] -> '):
        os.unlink('./aws-config/credentials')
        aws_login()
    return True

#Authenticate
aws_login()

# Init s3 resource
s3 = boto3.resource('s3')

#set output directory
output_dir = 'Extractions'

#set base directory
base_dir = os.path.join(os.path.abspath(os.curdir),output_dir)



def extract_bucket_contents(bucket_name,folder_name):
    """Extracts the contents of a given AWS s3 bucket

    Args:
        bucket_name (string): Name of the bucket
        folder_name (string): Subfolder name within the bucket
    """
    bucket =  s3.Bucket(bucket_name)
    objects = bucket.objects.filter(Prefix=folder_name)
    total_objects = sum(1 for _ in objects)
    if total_objects > 0:
        os.makedirs(f'{base_dir}/{bucket_name}/{folder_name}',exist_ok=True)
        os.chdir(f'{base_dir}/{bucket_name}/{folder_name}')
        print(f'\nExtracting {total_objects} object(s) from s3://{bucket_name}/{folder_name}...\n')
        with tqdm(total=total_objects,ncols=100,desc="Download Progress") as pbar:
            for obj in objects:
                pbar.update(1)
                path, filename = os.path.split(obj.key)
                if filename:
                    if not os.path.exists(filename):
                        bucket.download_file(obj.key,filename)
        os.chdir(base_dir)
        if confirm("\nWould you also like to compress the bucket contents to a zip file? [Y/N] -> "):
            print(f'\nWriting zip file {os.path.join(base_dir,bucket_name)}.zip...')
            make_archive(bucket_name,'zip',os.path.join(os.curdir,bucket_name))
    else:
        print(f'\nNo objects found at the given location: s3://{bucket_name}/{folder_name}')
    
def get_s3_target():
    """Retrives a s3 URI address from the user and 
    parses it to return the associated bucket name and folder\prefix path

    Returns:
        tuple: bucket name | folder path
    """
    s3_uri = input('\nEnter s3 URI (ex: s3://bucket_name/subfolder) -> ')
    while s3_uri == '':
        print('\n**s3 URI is required!**')
        s3_uri = input('\nEnter s3 URI (ex: s3://bucket_name/subfolder) -> ')
    try:
        bucket_name, folder_name = s3_uri.replace("s3://", "").split("/", 1)
        return bucket_name,folder_name.strip('*')
    except ValueError:
        print(f'\n** Error parsing s3 URI please try again **')


def main():
    try:
        print("\n\n|------🪣  S3 Bucket Extractor🪣------|\n")
        while True:
            bucket_name, folder_name = get_s3_target()
            extract_bucket_contents(bucket_name,folder_name)
    except Exception as e:
        print(f'\n** Error extracting bucket contents: {e} **')
        main()
    
    

if __name__ == '__main__':
    main()

