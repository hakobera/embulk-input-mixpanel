require "tmpdir"
require 'date'
require 'java'

require_relative "mixpanel/exporter"

module Embulk
  module Input

    class MixpanelInputPlugin < InputPlugin
      Plugin.register_input("mixpanel", self)

      SPECIAL_PREFIX = "$"
      @@logger = org.embulk.spi.Exec.getLogger("org.embulk.input.MixpanelInputPlugin")

      def self.transaction(config, &control)
        task = create_task(config)
        task["schema"] = config.param("columns", :array)

        replace_special_prefix = config.param("replace_special_prefix", :string, nil)

        columns = []
        task["schema"].each do |c|
          index = c["index"]
          name = c["name"]
          type = c["type"].to_sym

          if replace_special_prefix && name.start_with?(SPECIAL_PREFIX)
            name = replace_special_prefix + name[SPECIAL_PREFIX.length..-1]
          end

          columns << Column.new(index, name, type)
        end

        commit_report = yield(task, columns, 1)
        
        next_config_diff = {}
        return next_config_diff
      end

      def self.resume(task, columns, count, &control)
        raise "resume not supported"
      end

      def self.guess(config)
        task = create_task(config)
        columns = Guess::SchemaGuess.from_hash_records(sample_records(task))
        return {"columns" => columns}
      end

      def init
      end

      def run
        schema = task["schema"]

        MixpanelInputPlugin::export(task, MixpanelInputPlugin::local_cache_file_path(task)) do |record|
          row = Array.new(schema.length)
          schema.each do |col|
            row[col["index"]] = record[col["name"]]
          end
          page_builder.add(row)
        end
        page_builder.finish

        commit_report = {}
        return commit_report
      end

      private

      def self.create_task(config)
        task = {
          "mixpanel_api_key" => config.param("mixpanel_api_key", :string),
          "mixpanel_api_secret" => config.param("mixpanel_api_secret", :string),
          "event" => config.param("event", :string),
          "from_date" => config.param("from_date", :string),
          "to_date" => config.param("to_date", :string)
        }
      end
      
      def self.sample_records(task)
        path = local_cache_file_path(task)
        records = []

        export(task, path) do |record|
          records << record
        end

        records
      end

      def self.local_cache_file_path(task)
        prefix = "embulk-input-mixpanel-"
        key = Digest::SHA256.hexdigest(task.map{|key,val| "#{key}=#{val}"}.sort.join)
        path = "#{Dir.tmpdir}/#{prefix}#{key}"
        path
      end

      def self.export_to_local_cache_file(task, path)
        @@logger.info "start export to #{path}"

        exporter = Mixpanel::Exporter.new(task["mixpanel_api_key"], task["mixpanel_api_secret"])
        response = exporter.export(from_date: task["from_date"], to_date: task["to_date"], event: [task["event"]])       
          
        File.open(path, "w") do |f|
          response.parsed_response.each_line do |line|
            record = JSON.parse(line)
            props = record["properties"]
            f.puts JSON.generate(props)
          end
        end

        @@logger.info "end export to #{path}"
      end

      def self.export(task, path, &block)
        unless File.exists? path
          export_to_local_cache_file(task, path)
        end

        @@logger.info "load from #{path}"
        File.open(path) do |f|
          f.each do |line|
            yield JSON.parse(line)
          end
        end
      end
    end

  end
end
