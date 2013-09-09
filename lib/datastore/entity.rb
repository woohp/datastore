require 'active_support/core_ext/hash/indifferent_access'

module Datastore
  class Entity
    class << self
      attr_accessor :tx

      def find(id)
        resp = Datastore.execute(:lookup, {
          keys: [ self._make_key(id) ]
        })

        return nil if resp.data.found.empty?

        self._from_entity(resp.data.found[0].to_hash)
      end

      def all
        resp = Datastore.execute(:run_query, {
          query: {
            kinds: [ { name: self.name } ]
          }
        })

        resp.data.to_hash['batch']['entityResults'].map do |entity|
          self._from_entity(entity)
        end
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

      def create(attrs={})
        obj = self.new(attrs)
        obj.save
        obj
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
        obj['id'] = entity['key']['path'][0]['id'].to_i
        
        if entity.has_key?('properties')
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
      @data = ActiveSupport::HashWithIndifferentAccess.new

      attrs.each do |k, v|
        self[k] = v
      end
    end
  
    def save
      return nil unless self.valid?

      id = @data['id']
      if id.nil?
        resp = Datastore.execute(:blind_write, {
          mutation: {
            insertAutoId: [
              {
                key: {
                  path: [{
                    kind: self.class.name
                  }]
                },
                properties: self.class._make_properties(@data)
              }
            ]
          }
        })
        @data['id'] = resp.data.to_hash['mutationResult']['insertAutoIdKeys'][0]['path'][0]['id'].to_i
      else
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
      end

      true
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
      val = DateTime.parse(val.new_offset(0).rfc3339) if val.kind_of?(DateTime)
      @data[key] = val
    end
  
    def valid?
      (@data['id'].nil? or @data['id'].kind_of?(Integer)) and
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

    def ==(other)
      @data == other.data
    end
  end
end

