require 'spec_helper'

describe Postgres::Dblink::Sync do
  it 'has a version number' do
    expect(Postgres::Dblink::Sync::VERSION).not_to be nil
  end
end
