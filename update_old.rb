#!/usr/bin/env ruby
#-*- encoding: utf-8 -*-
require 'rubygems'
require 'zip'
require 'json'
require 'net/http'
require 'active_support/core_ext'
require 'action_view'
require 'httparty'
require 'nokogiri'


class FlibustaOld
  def initialize(path_to_torrent)
    @path_to_torrent = path_to_torrent
    @elastic = ARGV[1] || '127.0.0.1:9200'
    @bulk_size = 20
    @index = 'flibusta'
    @type = 'fb2'
  end

  def zips
    Dir.glob(File.join(@path_to_torrent, ['*.zip']))
  end

  def upload_to_elastic(content, id)
    id = id.gsub(' ', '_')
    url = "http://#{@elastic}/#{@index}/#{@type}/#{id}/_create"
    # puts HTTParty.put(url, body: JSON.dump({'content' => content})).body
  end

  def exists_in_elastic(id)
    url = "http://#{@elastic}/#{@index}/#{@type}/#{id}"
    HTTParty.head(url).response.class == Net::HTTPOK
  end

  def is_fb2name(name)
    name =~ /\.fb2\Z/
  end

  def fb2name_to_id(name)
    name =~ /(.*?)\.fb2\Z/
    $1.gsub(' ', '_')
  end

  def update
    zips_count = zips.count
    zips.each_with_index do |zip_name, zip_index|
      zip = Zip::File.open(zip_name)
      entries_count = zip.entries.count
      zip.entries.each_with_index do |entry, entry_index|
        next unless is_fb2name(entry.name)

        puts "#{File.basename(zip_name)}(#{zip_index}/#{zips_count}) #{entry.name}(#{entry_index}/#{entries_count})"

        _id = fb2name_to_id(entry.name)
        next if exists_in_elastic(_id)

        doc = Nokogiri::XML(entry.get_input_stream.read)
        doc.encoding = 'utf-8'

        description = doc.search('description').first.to_s
        content = doc.search('body').first.text rescue nil
        esdoc = {'description' => description,
                 'content' => content}

        url = "http://#{@elastic}/#{@index}/#{@type}/#{_id}/_create"
        # puts HTTParty.put(url, body: JSON.dump(esdoc)).body
      end
    end
  end

  def update_bulk
    commands = []
    commands_count = 0

    zips_count = zips.count
    zips.each_with_index do |zip_name, zip_index|
      zip = Zip::File.open(zip_name)
      entries_count = zip.entries.count
      zip.entries.each_with_index do |entry, entry_index|
        next unless is_fb2name(entry.name)

        puts "#{File.basename(zip_name)}(#{zip_index}/#{zips_count}) #{entry.name}(#{entry_index}/#{entries_count})"

        _id = fb2name_to_id(entry.name)
        next if exists_in_elastic(_id)

        doc = Nokogiri::XML(entry.get_input_stream.read)
        doc.encoding = 'utf-8'
        description = doc.search('description').first.to_s
        content = doc.search('body').first.text rescue nil

        commands << {:create => {:_id => _id}}
        commands << {:description => description, :content => content}
        commands_count += 1

        if commands_count == @bulk_size
          url = "http://#{@elastic}/#{@index}/#{@type}/_bulk"
          body = commands.map { |cmd| JSON.dump(cmd) }.join("\n") + "\n"
          # puts HTTParty.put(url, body: body).body

          commands = []
          commands_count = 0
        end
      end
    end
  end
end

path_to_torrent = ARGV[0] || File.expand_path(File.join(File.dirname(__FILE__), %w{.. fb2.Flibusta.Net}))
updater = Flibusta.new(path_to_torrent)
updater.update_bulk