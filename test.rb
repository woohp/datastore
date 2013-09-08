require './datastore'

Datastore.config(
  application_name: 'huipengs_company_data',
  application_version: '1.0.0',
  dataset_id: 'alert-diode-331',
  service_account: ENV['DATASTORE_SERVICE_ACCOUNT'],
  private_key_file: ENV['DATASTORE_PRIVATE_KEY_FILE']
)


class Company < Datastore::Entity
end

Company.transaction do
  companies = Company.where(fullname: "Google")
  companies.each do |c|
    c['value'] = 100
    c['updated_at'] = Time.now
    c.save
    p c
  end
end

## Get the entity by key.
#entity = Company.find 'goog'
#if entity.nil?
#  c = Company.new
#  c['fullname'] = "Google"
#  Company.transaction do
#    resp = c.save
#    p resp
#  end
#else
#  p entity
#end

