
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-kafka"
  spec.version       = "0.1.2"
  spec.authors       = ["Nobuaki Mochizuki"]
  spec.summary       = %[Kafka input plugin for Embulk]
  spec.description   = %[Loads records from Kafka.]
  spec.email         = ["hagaeru3sei@gmail.com"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/mochizuki_nobuaki/embulk-input-kafka"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
