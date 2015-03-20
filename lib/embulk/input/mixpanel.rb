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

        columns = []
        task["schema"].each do |c|
          index = c["index"]
          name = c["name"]
          type = c["type"].to_sym
          format = c["format"]

          if task["replace_special_prefix"] && name.start_with?(SPECIAL_PREFIX)
            name = task["replace_special_prefix"] + name[SPECIAL_PREFIX.length..-1]
          end

          columns << Column.new(index, name, type, format)
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
            val = record[col["name"]]
            row[col["index"]] =
              if val.nil?
                nil
              else
                case col["type"].to_sym
                when :boolean, :long, :double
                  val
                when :string
                  val.to_s
                when :timestamp
                  if col["format"]
                    Time.strptime(val, col["format"])
                  elsif val.respond_to(:to_i)
                    Time.at(val.to_i)
                  else
                    Time.parse(val)
                  end
                else
                  raise "unknown type: #{col['type']}"
                end
              end
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
          "to_date" => config.param("to_date", :string),
          "replace_special_prefix" => config.param("replace_special_prefix", :string, nil)
        }
        task["digest"] = Digest::SHA256.hexdigest(task.map{|key,val| "#{key}=#{val}"}.sort.join)
        task
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
        path = "#{Dir.tmpdir}/#{prefix}#{task['digest']}"
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
      rescue
        File.delete(path) if File.exists?(path)
      end

      def self.export(task, path, &block)
        export_to_local_cache_file(task, path) unless File.exists? path

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
