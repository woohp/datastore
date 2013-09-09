require 'datastore/entity'
require 'date'


class Test < Datastore::Entity
end

describe Datastore::Entity do
  before(:all) do
    Test.all.each do |t|
      t.destroy
    end
  end

  it "queries_empty_table" do
    objs = Test.all
    objs.should respond_to(:each)
    objs.size.should eq 0
  end

  it "validate objects correctly" do
    t = Test.new
    t['id'].should be_nil
    t.valid?.should be_true

    t['a'] = 1
    t['b'] = '2'
    t['c'] = 3.0
    t['d'] = true
    t['e'] = false
    t['f'] = DateTime.now
    t.valid?.should be_true

    t['g'] = []
    t.valid?.should be_false

    t['g'] = nil
    t.valid?.should be_false
    t['g'] = 1

    t['id'] = 'invalid_id'
    t.valid?.should be_false
  end

  it "creates and destroys single objects" do
    t = Test.new
    t['id'].should be_nil
    t.save.should be_true
    t['id'].should be_a_kind_of(Integer)

    Test.all.size.should eq 1

    t['a'] = 1
    t.save

    tests = Test.all
    tests.size.should eq 1

    t2 = tests[0]
    t2.should eq(t)

    t3 = Test.find(t['id'])
    t3.should eq(t)

    t.destroy
    Test.all.size.should eq 0
  end

  it "types of objects remain consistent" do
    t = Test.create(a: 1, b: '2', c: 3.0, d: true, e: false, f: DateTime.now)
    t.save
    t2 = Test.find(t['id'])
    t2.should eq(t)

    t2['a'].should be_a_kind_of(Integer)
    t2['b'].should be_a_kind_of(String)
    t2['c'].should be_a_kind_of(Float)
    t2['d'].should be_a_kind_of(TrueClass)
    t2['e'].should be_a_kind_of(FalseClass)
    t2['f'].should be_a_kind_of(DateTime)

    t.destroy
  end

  it "creates and destroys multiple objects" do
    tests = []
    5.times do
      tests << Test.create(a: 1)
    end

    Test.all.size.should be(5)
    
    tests.each do |t|
      t.destroy
    end

    Test.all.size.should be(0)
  end

  it "queries correctly" do
    t1 = Test.create(a: 1)
    t2 = Test.create(a: 1)
    t3 = Test.create(a: 1)
    t4 = Test.create(a: 2)
    t5 = Test.create(a: 3)

    test1s = Test.where(a: 1)
    test1s.size.should be(3)
    test1s.all? do |t|
      t['a'] == 1
    end

    test2s = Test.where(a: 2)
    test2s.size.should be(1)
    test2s.all? do |t|
      t['a'] == 2
    end

    test3s = Test.where(a: 3)
    test3s.size.should be(1)
    test3s.all? do |t|
      t['a'] == 3
    end

    test1s_wrong = Test.where(a: '1')
    test1s_wrong.size.should be(0)

    t1.destroy
    t2.destroy
    t3.destroy
    t4.destroy
    t5.destroy
  end
end
