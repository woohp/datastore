require 'datastore'
require 'signet'

describe Datastore do
  it "fails to config without all arguments" do
    expect { Datastore.config }.to raise_error ArgumentError

    expect {
      Datastore.config(
        application_name: 'foo',
        appilcation_version: '1.0.0'
      )
    }.to raise_error ArgumentError

    expect {
      Datastore.config(
        application_name: 'foo',
        application_version: '1.0.0',
        dataset_id: 'foo',
        service_account: 'foo'
      )
    }.to raise_error ArgumentError

    expect {
      Datastore.config(
        application_name: 'foo',
        application_version: '1.0.0',
        dataset_id: 'foo',
        private_key_file: 'foo'
      )
    }.to raise_error ArgumentError
  end


  it "fails to config without accurate arguments" do
    expect {
      Datastore.config(
        application_name: 'foo',
        application_version: '1.0.0',
        dataset_id: 'foo',
        service_account: 'foo',
        private_key_file: 'foo'
      )
    }.to raise_error ArgumentError

    # invalid private key
    expect {
      Datastore.config(
        application_name: 'huipengs_company_data',
        application_version: '1.0.0',
        dataset_id: 'alert-diode-331',
        service_account: ENV['DATASTORE_SERVICE_ACCOUNT'], 
        private_key_file: 'foo'
      )
    }.to raise_error ArgumentError

    # invalid login
    expect {
      Datastore.config(
        application_name: 'huipengs_company_data',
        application_version: '1.0.0',
        dataset_id: 'alert-diode-331',
        service_account: 'foo',
        private_key_file: ENV['DATASTORE_PRIVATE_KEY_FILE']
      )
    }.to raise_error Signet::AuthorizationError
  end

  it "configs successfully given good arguments" do
    Datastore.config(
      application_name: 'huipengs_company_data',
      application_version: '1.0.0',
      dataset_id: 'alert-diode-331',
      service_account: ENV['DATASTORE_SERVICE_ACCOUNT'],
      private_key_file: ENV['DATASTORE_PRIVATE_KEY_FILE']
    ).schemas.size.should be > 0
  end
end
