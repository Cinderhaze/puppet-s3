# todo: self.instances and self.prefetch not yet implemented. 
# Currently this provider compares the MD5 hash of the S3 and the local file system file, if that comparison returns 
# false then the S3 object is pulled and written to the :path on the filesystem.
#
# Example:
#   s3 { '/path/to/my/filesystem':
#       ensure              => present,
#       source              => '/bucket/path/to/object',
#       access_key_id       => 'mysecret',
#       secret_access_key   => 'anothersecret',
#       region              => 'us-west-1', # Defaults to us-east-1
#   }
#
#   Author: jeff malnick, malnick@gmail.com

require 'rubygems' if Puppet.features.rubygems? 
require 'aws-sdk' if Puppet.features.awssdk?
require 'digest'
require 'tempfile'

Puppet::Type.type(:s3).provide(:s3) do
  confine :feature => :awssdk
  confine :feature => :rubygems

  desc "Securely get shit out of S3. Note this provider requires Version 2 of the aws-sdk. Ensure that v2 is installed."

  def create
    Puppet.info('Connecting to AWS S3')   
    # TODO set it up to work even if you don't provide access and secret keys (automatic credintial lookup on ec2 or ~/.aws/credentials ?
    s3 = Aws::S3::Client.new( 
        :access_key_id      => resource[:access_key_id], 
        :secret_access_key  => resource[:secret_access_key],
        :region             => resource[:region] || 'us-east-1',
    )

    # Get the name of the bucket and path to the object:
    source_ary  = resource[:source].chomp.split('/')
    source_ary.shift # Remove prefixed white space
    
    bucket      = source_ary.shift
    key         = File.join(source_ary)
    
    Puppet.info("Pulling bucket: #{bucket}, key: #{key}")
    # Handle new S3 object
    resp = s3.get_object(
        response_target:    resource[:path],
        bucket:             bucket,
        key:                key,
    )

    # Create a .etag file when we've downloaded an artifact
    IO.write(resource[:path], resp.etag) #TODO any kind of exception handling required?
    
  end

  def destroy

      # rm rf some file on the filesystem that points to resource[:path]
    
  end

  # TODO Add condition where the ETag isn't cached, but the head_object has
  #      an ETag that is an MD5 (aka, does not match /-\d$/
  def exists?


      # Create a new S3 client object
      s3 = Aws::S3::Client.new( 
          :access_key_id      => resource[:access_key_id], 
          :secret_access_key  => resource[:secret_access_key],
          :region             => resource[:region] || 'us-east-1',
      )
            
      # Do all the same stuff I did for create
      source_ary  = resource[:source].chomp.split('/')
      source_ary.shift # Remove prefixed white space
            
      bucket      = source_ary.shift
      key         = File.join(source_ary)

      if File.exists?(resource[:path] + '.etag')  
          # Read in the cached etag for the file 
          cached_etag = IO.read(resource[:path] + '.etag')
        
          # Fetch the current metadata for the file
          resp = s3.head_object(
              bucket:             bucket,
              key:                key,
          )

          fresh_etag = resp.etag

          # Compare the current metadata's etag with the cached etag 
          if cached_etag  == fresh_etag 
              true
          else
              false
          end
      
      # No etag cached file exists, redownload the file and compare MD5
      elsif File.exists?(resource[:path])  

          # Setup a temp file to compare against
          temp_file = Tempfile.new(resource[:path])

            Puppet.info('Setting new S3 object and downloading...')

            # Grab the object and point it at temp_file
            resp = s3.get_object(
                response_target:    temp_file, 
                bucket:             bucket,
                key:                key,
            )
            
            # Compare the MD5 hashes, return true or false 
            temp_file_md5   = Digest::MD5.file(temp_file).hexdigest 
            actual_file_md5 = Digest::MD5.file(resource[:path]).hexdigest

            if temp_file_md5  == actual_file_md5 
                true
            else
                false
            end
      end
  end

end
