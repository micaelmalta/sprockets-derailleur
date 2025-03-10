require "sprockets"

module Sprockets
  class Manifest
    alias_method :compile_with_workers, :compile

    def compile(*args)
      SprocketsDerailleur::prepend_file_store_if_required

      worker_count = SprocketsDerailleur::worker_count
      paths_with_errors = {}

      time = Benchmark.measure do
        env = Rails.application.assets
        paths = args.flatten.select { |fn| Pathname.new(fn).absolute? if fn.is_a?(String) }

        # Ensure we collect assets correctly in Sprockets 4
        if env.respond_to?(:each_file)
          paths += env.each_file.to_a
        elsif env.respond_to?(:each_logical_path)
          paths += env.each_logical_path(*args).to_a
        end

        paths.reject! do |path|
          if File.extname(path).empty?
            logger.info "Skipping #{path} since it has no extension"
            true
          else
            false
          end
        end

        logger.warn "Initializing #{worker_count} workers"

        workers = []
        worker_count.times do
          workers << worker(paths)
        end

        reads = workers.map { |worker| worker[:read] }
        writes = workers.map { |worker| worker[:write] }

        index = 0
        finished = 0

        loop do
          break if finished >= paths.size

          ready = IO.select(reads, writes)

          ready[0].each do |readable|
            data = Marshal.load(readable)
            assets.merge!(data["assets"])
            files.merge!(data["files"])
            paths_with_errors.merge!(data["errors"])

            finished += 1
          end

          ready[1].each do |write|
            break if index >= paths.size

            Marshal.dump(index, write)
            index += 1
          end
        end

        logger.debug "Cleaning up workers"

        workers.each do |worker|
          worker[:read].close
          worker[:write].close
        end

        workers.each { |worker| Process.wait(worker[:pid]) }

        save
      end

      logger.warn "Completed compiling assets (#{(time.real * 100).round / 100.0}s)"

      unless paths_with_errors.empty?
        logger.warn "Asset paths with errors:"
        paths_with_errors.each { |path, message| logger.warn "\t#{path}: #{message}" }
      end
    end

    def worker(paths)
      child_read, parent_write = IO.pipe
      parent_read, child_write = IO.pipe

      pid = fork do
        begin
          parent_write.close
          parent_read.close

          while !child_read.eof?
            path = paths[Marshal.load(child_read)]

            time = Benchmark.measure do
              data = { 'assets' => {}, 'files' => {}, 'errors' => {} }

              version_agnostic_find(path).each do |asset|
                data['files'][asset.digest_path] = {
                  'logical_path' => asset.logical_path,
                  'mtime'        => asset.mtime.iso8601,
                  'size'         => asset.length,
                  'digest'       => asset.digest
                }
                data['assets'][asset.logical_path] = asset.digest_path

                target = File.join(dir, asset.digest_path)

                if File.exist?(target)
                  logger.debug "Skipping #{target}, already exists"
                else
                  logger.info "Writing #{target}"
                  asset.write_to(target)
                  asset.write_to("#{target}.gz") unless skip_gzip?(asset)
                end

                Marshal.dump(data, child_write)
              end
            end

            log_compile_time(path, time)
          end
        ensure
          child_read.close
          child_write.close
        end
      end

      child_read.close
      child_write.close

      { read: parent_read, write: parent_write, pid: pid }
    end

    private

    def version_agnostic_find(*args)
      if sprockets2?
        [find_asset(*args)].each
      elsif sprockets4?
        environment.find_all_linked_assets(*args)
      else
        find(*args)
      end
    end

    def sprockets2?
      Sprockets::VERSION.start_with?('2')
    end

    def sprockets4?
      Sprockets::VERSION.start_with?('4')
    end

    def skip_gzip?(asset)
      return !asset.is_a?(BundledAsset) if sprockets2?
      return !environment.respond_to?(:skip_gzip?) || environment.skip_gzip?
    end

    def log_compile_time(path, time)
      message = "Compiled #{path} (#{(time.real * 1000).round}ms, pid #{Process.pid})"
      if SprocketsDerailleur.configuration.compile_times_to_info_log
        logger.info message
      else
        logger.debug message
      end
    end
  end
end
