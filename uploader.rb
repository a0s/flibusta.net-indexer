require 'rubygems'
require 'action_view'
require 'elasticsearch'
require 'active_support/all'
require 'active_support/number_helper'
require 'nokogiri'
require 'zip'
require 'pp'

class Flibusta
  include ActionView::Helpers::NumberHelper

  def initialize
    @elastic = Elasticsearch::Client.new log: false,
                                         :retry_on_failure => 10,
                                         :reload_connections => 100,
                                         :transport_options => {
                                             :request => {
                                                 :timeout => 60,
                                                 :open_timeout => 30
                                             }
                                         }

    @bulk_size = ENV['SIZE'].present? ? ENV['SIZE'].to_i : 53
  end

  def process_fb2(content)
    doc = Nokogiri::XML(content)
    doc.encoding = 'utf-8'
    content = doc.search('body').first.text.unpack('U*').pack('U*').gsub(/\s+/, ' ').strip rescue nil
    description = doc.search('description').first.text.unpack('U*').pack('U*').gsub(/\s+/, ' ').strip rescue nil
    [content, description]
  end

  def upload_folder_with_zips(start_folder)
    zip_mask = File.expand_path(start_folder) + '/*.zip'
    zip_files = Dir[zip_mask].map { |f| f }
    zip_files_count = zip_files.size
    zip_files.each_with_index do |zip_file, zip_file_index|
      zip = Zip::File.open(zip_file)
      entries_count = zip.entries.count
      entry_index = 0
      zip.entries.each_slice(@bulk_size) do |slice_entries|

        bulk_body = []

        slice_entries.each do |entry|
          entry_index += 1
          next unless entry.name.end_with?('.fb2')

          id = entry.name.chomp('.fb2')
          if @elastic.exists index: 'books', type: 'book', id: id
            puts "Exists #{id}"
            next
          end

          raw_zip = entry.get_input_stream.read
          content, description = process_fb2(raw_zip)
          human_size = number_to_human_size(content.size)
          zip_file_name = File.basename(zip_file)
          bulk_body << {
              index: {
                  _index: 'books',
                  _type: 'book',
                  _id: id,
                  data: {
                      content: content,
                      description: description,
                      zip_file: zip_file_name
                  }
              }
          }


          puts "id:#{id} #{human_size} #{zip_file} (#{zip_file_index}/#{zip_files_count}) -> #{entry.name} (#{entry_index}/#{entries_count})"
        end

        if bulk_body.present?
          puts 'Upload bulk .. '
          result = @elastic.bulk body: bulk_body, timeout: 120, replication: 'async', refresh: false
          puts result if result['errors']
        end
      end
    end
  end

end

fb = Flibusta.new
fb.upload_folder_with_zips(ARGV[0])