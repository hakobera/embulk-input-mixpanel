require "httparty"

module Embulk
  module Input
    module Mixpanel
      class Exporter
        include HTTParty

        base_uri "https://data.mixpanel.com"
        format :plain

        def initialize(api_key, api_secret)
          @api_key = api_key
          @api_secret = api_secret
        end

        def export(args)
          params = args.clone
          params["api_key"] = @api_key
          params["format"] = 'json'
          params["expire"] = Time.now.to_i + 1800
          params["sig"] = generate_signature(params)

          self.class.get("https://data.mixpanel.com/api/2.0/export/?" + encode(params))
        end

        def encode(params)
          params.map{|key,val| "#{key}=#{CGI.escape(val.to_s)}"}.sort.join('&')
        end

        def generate_signature(args)
          Digest::MD5.hexdigest(args.map{|key,val| "#{key}=#{val}"}.sort.join + @api_secret)
        end
      end
    end
  end
end
