require 'elasticsearch'
require 'active_support/all'
require 'nokogiri'
require 'zip'
require 'pp'

class Flibusta
  def initialize
    @elastic = Elasticsearch::Client.new log: false
    @elastic.transport.reload_connections!

    @bulk_size = ENV['SIZE'].present? ? ENV['SIZE'].to_i : 37
  end

  def process_fb2(content)
    doc = Nokogiri::XML(content)
    doc.encoding = 'utf-8'
    content = doc.search('body').first.text.unpack('U*').pack('U*').gsub(/\s+/, ' ').strip rescue nil
    description = doc.search('description').first.text.unpack('U*').pack('U*').gsub(/\s+/, ' ').strip rescue nil
    [content, description]
  end

  def upload_bulk(zip_file, fb_files, global_index, global_total)
    bulk_body = []

    total = fb_files.size
    fb_files.each_with_index do |fb_file, index|
      unless fb_file.end_with?('.fb2')
        puts "Not fb2 #{fb_file}"
        next
      end

      id = File.basename(fb_file).chomp('.fb2')

      raw = File.read(fb_file)
      content, description = process_fb2(raw)

      bulk_body << {
          index: {
              _index: 'books',
              _type: 'book',
              _id: id,
              data: {
                  content: content,
                  description: description,
                  zip_file: zip_file
              }
          }
      }

      puts "#{index + global_index}(#{total})/#{global_total}"
    end

    puts 'Upload bulk .. '
    result = @elastic.bulk body: bulk_body, timeout: 120, replication: 'async', refresh: false
    if result['errors']
      puts result
    end
    nil
  end

  def upload_dir(dir)
    path = File.expand_path(dir)
    zip_file = File.basename(path)
    files = Dir[path + '/*'].map { |e| e }
    total = files.size
    uploaded = 0
    files.each_slice(@bulk_size) do |slice_files|
      upload_bulk(zip_file, slice_files, uploaded, total)
      uploaded += slice_files.size
      puts "#{uploaded}/#{total}"
    end
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

          puts "id:#{id} #{zip_file} (#{zip_file_index}/#{zip_files_count}) -> #{entry.name} (#{entry_index}/#{entries_count})"
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
# fb.upload_dir(ARGV[0])
fb.upload_folder_with_zips(ARGV[0])