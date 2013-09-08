require 'google/api_client'
require 'securerandom'
require 'date'


module Datastore
  @dataset_id = nil
  @client = nil
  @datastore = nil

  def self.config(config={})
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


  class Entity
    class << self
      attr_accessor :tx

      def find(id)
        resp = Datastore.execute(:lookup, {
          keys: self._make_key(id)
        })

        return nil if resp.data.found.empty?

        self._from_entity(resp.data.found[0].to_hash)
      end

      def where(filters)
        resp = Datastore.execute(:run_query, {
          query: {
            kinds: [ { name: self.name } ],
            filter: {
              compositeFilter: {
                operator: 'and',
                filters: filters.map do |k, v|
                  {
                    propertyFilter: {
                      operator: 'equal',
                      property: { name: k },
                      value: self._make_value(v)
                    }
                  }
                end
              }
            }
          }
        })

        resp.data.to_hash['batch']['entityResults'].map do |entity|
          self._from_entity(entity)
        end
      end

      def create
      end
  
      def transaction(&block)
        # begin transaction
#        resp = Datastore.execute(:begin_transaction)
#        self.tx = resp.data['transaction', true] # Get the transaction handle

        block.call()

        # end transaction
#        resp = Datastore.execute(:commit)
#        self.tx = nil
      end

      def _from_entity(entity)
        obj = self.new

        entity = entity['entity']
        obj['id'] = entity['key']['path'][0]['id']
        entity['properties'].each do |key, val|
          val = val['values'][0]

          if val.has_key? 'stringValue'
            val = val['stringValue']
          elsif val.has_key? 'integerValue'
            val = val['integerValue'].to_i
          elsif val.has_key? 'doubleValue'
            val = val['doubleValue'].to_f
          elsif val.has_key? 'booleanValue'
            val = val['booleanValue']
          elsif val.has_key? 'dateTimeValue'
            val = DateTime.parse(val['dateTimeValue'])
          end

          obj[key] = val
        end

        obj
      end

      def _make_properties(hash)
        properties = {}

        hash.each do |key, val|
          next if key == 'id'

          properties[key] = { values: [self._make_value(val)] }
        end

        properties
      end

      def _make_value(val)
        if val.kind_of? String
          { stringValue: val }
        elsif val.kind_of? Integer
          { integerValue: val }
        elsif val.kind_of? Float
          { doubleValue: val }
        elsif val.kind_of? TrueClass or val.kind_of? FalseClass
          { booleanValue: val }
        elsif val.kind_of? Time
          { dateTimeValue: val.to_datetime }
        elsif val.kind_of? DateTime
          { dateTimeValue: val }
        end
      end

      def _make_key(id)
        {
          path: [{
            kind: self.name,
            id: id
          }]
        }
      end
    end
  

    attr_accessor :data
  
    def initialize(attrs={})
      @data = {}
    end
  
    def save
      id = @data['id'] || Integer(SecureRandom.hex, 16)

      Datastore.execute(:blind_write, {
        mutation: {
          upsert: [
            {
              key: self.class._make_key(id),
              properties: self.class._make_properties(@data)
            }
          ]
        }
      })

      @data['id'] = id
    end

    def destroy
      return if @data['id'].nil?

      Datastore.execute(:blind_write, {
        mutation: {
          delete: [ self.class._make_key(@data['id']) ]
        }
      })
    end
  
    def [](key)
      @data[key]
    end
  
    def []=(key, val)
      @data[key] = val
    end
  
    def valid?
      @data.values.all? do |val|
        not val.nil? and
          (val.kind_of?(String) or
           val.kind_of?(Integer) or
           val.kind_of?(Float) or
           val.kind_of?(TrueClass) or
           val.kind_of?(FalseClass) or
           val.kind_of?(Time) or
           val.kind_of?(DateTime)
          )
      end
    end
  end
end


