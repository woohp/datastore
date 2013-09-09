require 'google/api_client'
require 'date'


module Datastore
  @dataset_id = nil
  @client = nil
  @datastore = nil

  def self.config(config={})
    if config[:application_name].nil? or config[:application_version].nil? or
      config[:dataset_id].nil? or config[:service_account].nil? or config[:private_key_file].nil?
      raise ArgumentError
    end

    @dataset_id = config[:dataset_id]

    @client = Google::APIClient.new(
      application_name: config[:application_name],
      application_version: config[:application_version]
    )

    private_key = Google::APIClient::KeyUtils.load_from_pkcs12(config[:private_key_file],
                                                               'notasecret')
    @client.authorization = Signet::OAuth2::Client.new(
      token_credential_uri: 'https://accounts.google.com/o/oauth2/token',
      audience: 'https://accounts.google.com/o/oauth2/token',
      scope: ['https://www.googleapis.com/auth/datastore',
              'https://www.googleapis.com/auth/userinfo.email'],
      issuer: config[:service_account],
      signing_key: private_key)
    # Authorize the client.
    @client.authorization.fetch_access_token!

    # Build the datastore API client.
    @datastore = @client.discovered_api('datastore', 'v1beta1')
  end

  def self.execute(method, body={})
    method = case method
             when :allocate_ids then @datastore.datasets.allocate_ids
             when :begin_transaction then @datastore.datasets.begin_transaction
             when :blind_write then @datastore.datasets.blind_write
             when :commit then @datastore.datasets.commit
             when :lookup then @datastore.datasets.lookup
             when :rollback then @datastore.datasets.rollback
             when :run_query then @datastore.datasets.run_query
             end
    @client.execute(
      api_method: method,
      parameters: {
        datasetId: @dataset_id
      },
      body_object: body
    )
  end
end


require 'datastore/entity'
