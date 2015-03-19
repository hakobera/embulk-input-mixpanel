
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-mixpanel"
  spec.version       = "0.1.0"
  spec.authors       = ["Kazuyuki Honda"]
  spec.summary       = "Mixpanel input plugin for Embulk"
  spec.description   = "Loads records from Mixpanel."
  spec.email         = ["hakobera@gmail.com"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/hakobera/embulk-input-mixpanel"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'httparty', '~> 0.13.3'
  
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
  spec.add_development_dependency 'test-unit', ['>= 0.3.2']
end
