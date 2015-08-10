require "postgres_to_redshift/version"
require 'pg'
require 'uri'
require 'aws-sdk'
require 'zlib'
require 'stringio'
require "postgres_to_redshift/table"
require "postgres_to_redshift/column"
require 'dotenv'
require 'database_url'

Dotenv.load

class PostgresToRedshift
  CONFIG = {
    s3_bucket: ENV['S3_DATABASE_EXPORT_BUCKET'],
    s3_key: ENV['S3_DATABASE_EXPORT_KEY'],
    s3_id: ENV['S3_DATABASE_EXPORT_ID'],
    schema: ENV['TARGET_SCHEMA'],
    source_uri: ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI'],
    tables_to_export: ENV['TABLES_TO_EXPORT'],
    target_uri: ENV['POSTGRES_TO_REDSHIFT_TARGET_URI'],
  }

  def initialize
  end

  def run
    target_connection.exec('CREATE SCHEMA IF NOT EXISTS #{target_schema}')

    tables.each do |table|
      target_connection.exec('CREATE TABLE IF NOT EXISTS #{target_schema}.#{table.target_table_name} (#{table.columns_for_create})')

      export_table(table)

      import_table(table)
    end
  end

  private

  def target_schema
    @target_schema ||= CONFIG[:schema] || 'public'
  end

  def source_connection
    if @source_connection.nil?
      @source_connection ||= PG::Connection.new(source_connection_params)
      @source_connection.exec('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;')
    end

    @source_connection
  end

  def target_connection
    @target_connection ||= PG::Connection.new(target_connection_params)
  end

  def source_connection_params
    @source_uri ||= URI.parse(CONFIG[:source_uri])

    uri_to_params(@source_uri)
  end

  def target_connection_params
    @target_uri ||= URI.parse(CONFIG[:target_uri])

    uri_to_params(@target_uri)
  end

  def uri_to_params(uri)
    {
      host: uri.host,
      port: uri.port,
      user: uri.user,
      password: uri.password,
      dbname: uri.path[1..-1]
    }
  end

  def export_table?(table)
    @tables_to_export ||= CONFIG[:tables_to_export].nil? ? [] : CONFIG[:tables_to_export].split(',')

    return false if table.name =~ /^pg_/

    @tables_to_export.empty? || @tables_to_export.include?(table.name)
  end

  def tables
    source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE', 'VIEW')").map do |table_attributes|
      table = Table.new(attributes: table_attributes)

      next unless export_table?(table)
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}' order by ordinal_position")
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: CONFIG[:s3_id], secret_access_key: CONFIG[:s3_key])
  end

  def bucket
    @bucket ||= s3.buckets[CONFIG[:s3_bucket]]
  end

  def export_table(table)
    buffer = StringIO.new
    zip = Zlib::GzipWriter.new(buffer)

    puts "Downloading #{table}"
    copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{table.name}) TO STDOUT WITH DELIMITER '|'"

    source_connection.copy_data(copy_command) do
      while row = source_connection.get_copy_data
        zip.write(row)
      end
    end

    zip.finish
    buffer.rewind
    upload_table(table, buffer)
  end

  def upload_table(table, buffer)
    puts "Uploading #{table.target_table_name}"
    bucket.objects["export/#{table.target_table_name}.psv.gz"].delete
    bucket.objects["export/#{table.target_table_name}.psv.gz"].write(buffer, acl: :authenticated_read)
  end

  def import_table(table)
    puts "Importing #{table.target_table_name}"
    target_connection.exec("DROP TABLE IF EXISTS #{target_schema}.#{table.target_temp_table_name}")

    target_connection.exec("CREATE TABLE #{target_schema}.#{table.target_temp_table_name} (#{table.columns_for_create})")

    target_connection.exec("COPY #{target_schema}.#{table.target_temp_table_name} FROM 's3://#{CONFIG[:s3_bucket]}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

    target_connection.exec("BEGIN;")

    target_connection.exec("DROP TABLE IF EXISTS #{target_schema}.#{table.target_table_name}")

    target_connection.exec("ALTER TABLE #{target_schema}.#{table.target_temp_table_name} RENAME TO #{table.target_table_name}")

    target_connection.exec("COMMIT;")
  end
end
