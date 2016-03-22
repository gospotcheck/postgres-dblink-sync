# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'postgres/dblink/sync/version'

Gem::Specification.new do |spec|
  spec.name          = "postgres-dblink-sync"
  spec.version       = Postgres::Dblink::Sync::VERSION
  spec.authors       = ["Chris Schenk"]
  spec.email         = ["chrisschenk@gospotcheck.com"]

  spec.summary       = %q{Defines a base class to assist with synchronizing data from a remote Postgres database into a local database with the ability to define the query to be run on the remote server}
  spec.description   = %q{Synchronize data into your local database from a remote}
  spec.homepage      = "https://github.com/gospotcheck/postgres-dblink-sync"
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org by setting 'allowed_push_host', or
  # delete this section to allow pushing this gem to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.10"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "activesupport", "~> 4.2.4"
  spec.add_development_dependency "pg", "~> 0.18.4"
end
