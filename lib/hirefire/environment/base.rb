# encoding: utf-8

module HireFire
  module Environment
    class Base

      ##
      # Include HireFire::Backend helpers
      include HireFire::Backend

      ##
      # This method gets invoked when a new job has been queued
      #
      # Iterates through the default (or user-defined) job/worker ratio until
      # it finds a match for the for the current situation (see example).
      #
      # @example
      #   # Say we have 40 queued jobs, and we configured our job/worker ratio like so:
      #
      #   HireFire.configure do |config|
      #     config.max_workers      = 5
      #     config.min_workers      = 0
      #     config.job_worker_ratio = [
      #       { :jobs => 1,   :workers => 1 },
      #       { :jobs => 15,  :workers => 2 },
      #       { :jobs => 35,  :workers => 3 },
      #       { :jobs => 60,  :workers => 4 },
      #       { :jobs => 80,  :workers => 5 }
      #     ]
      #   end
      #
      #   # It'll match at { :jobs => 35, :workers => 3 }, (35 jobs or more: hire 3 workers)
      #   # meaning that it'll ensure there are 3 workers running.
      #
      #   # If there were already were 3 workers, it'll leave it as is.
      #
      #   # Alternatively, you can use a more functional syntax, which works in the same way.
      #
      #   HireFire.configure do |config|
      #     config.max_workers = 5
      #     config.job_worker_ratio = [
      #       { :when => lambda {|jobs| jobs < 15 }, :workers => 1 },
      #       { :when => lambda {|jobs| jobs < 35 }, :workers => 2 },
      #       { :when => lambda {|jobs| jobs < 60 }, :workers => 3 },
      #       { :when => lambda {|jobs| jobs < 80 }, :workers => 4 }
      #     ]
      #   end
      #
      #   # If there were more than 3 workers running (say, 4 or 5), it will NOT reduce
      #   # the number. This is because when you reduce the number of workers, you cannot
      #   # tell which worker Heroku will shut down, meaning you might interrupt a worker
      #   # that's currently working, causing the job to fail. Also, consider the fact that
      #   # there are, for example, 35 jobs still to be picked up, so the more workers,
      #   # the faster it processes. You aren't even paying more because it doesn't matter whether
      #   # you have 1 worker, or 5 workers processing jobs, because workers are pro-rated to the second.
      #   # So basically 5 workers would cost 5 times more, but will also process 5 times faster.
      #
      #   # Once all jobs finished processing (e.g. Delayed::Job.jobs == 0), HireFire will invoke a signal
      #   # which will set the workers back to 0 and shuts down all the workers simultaneously.
      #
      # @return [nil]
      def hire(type)
        jobs_count    = jobs(type)
        current_workers_count = workers(type) || return

        
        # ##
        # # Use "Standard Notation"
        # if not ratio.first[:when].respond_to? :call
        # 
        #   ##
        #   # Since the "Standard Notation" is defined in the in an ascending order
        #   # in the array of hashes, we need to reverse this order in order to properly
        #   # loop through and break out of the array at the correctly matched ratio
        #   ratio.reverse!
        # 
        #   ##
        #   # Iterates through all the defined job/worker ratio's
        #   # until it finds a match. Then it hires (if necessary) the appropriate
        #   # amount of workers and breaks out of the loop
        #   ratio.each do |ratio|
        # 
        #     ##
        #     # Standard notation
        #     # This is the code for the default notation
        #     #
        #     # @example
        #     #   { :jobs => 35,  :workers => 3 }
        #     #
        #     if jobs_count >= ratio[:jobs] and max_workers >= ratio[:workers]
        #       if workers_count < ratio[:workers]
        #         log_and_hire(ratio[:workers])
        #       end
        # 
        #       return
        #     end
        #   end
        # 
        #   ##
        #   # If no match is found in the above job/worker ratio loop, then we'll
        #   # perform one last operation to see whether the the job count is greater
        #   # than the highest job/worker ratio, and if this is the case then we also
        #   # check to see whether the maximum amount of allowed workers is greater
        #   # than the amount that are currently running, if this is the case, we are
        #   # allowed to hire the max amount of workers.
        #   if jobs_count >= ratio.first[:jobs] and max_workers > workers_count
        #     log_and_hire(max_workers)
        #     return
        #   end
        # end

        ##
        # Use "Functional (Lambda) Notation"
        # if ratio.first[:when].respond_to? :call
        # 
        #   ##
        #   # Iterates through all the defined job/worker ratio's
        #   # until it finds a match. Then it hires (if necessary) the appropriate
        #   # amount of workers and breaks out of the loop
        #   ratio.each do |ratio|
        # 
        #     ##
        #     # Functional (Lambda) Notation
        #     # This is the code for the Lambda notation, more verbose,
        #     # but more humanly understandable
        #     #
        #     # @example
        #     #   { :when => lambda {|jobs| jobs < 60 }, :workers => 3 }
        #     #
        #     if ratio[:when].call(jobs_count) and max_workers >= ratio[:workers]
        #       if workers_count < ratio[:workers]
        #         log_and_hire(ratio[:workers])
        #       end
        # 
        #       break
        #     end
        #   end
        # end

        ##
        # Applies only to the Functional (Lambda) Notation
        # If the amount of queued jobs exceeds that of which was defined in the
        # job/worker ratio array, it will hire the maximum amount of workers
        #
        # "if statements":
        #   1. true if we use the Functional (Lambda) Notation
        #   2. true if the last ratio (highest job/worker ratio) was exceeded
        #   3. true if the max amount of workers are not yet running
        #
        # If all the the above statements are true, HireFire will hire the maximum
        # amount of workers that were specified in the configuration
        #
        # if ratio.last[:when].respond_to? :call \
        # and ratio.last[:when].call(jobs_count) === false \
        # and max_workers != workers_count
        #   log_and_hire(max_workers)
        # end
        type = type.to_sym
        
        count = worker_count[type]
        
        if worker_count[type] == :scale
          if jobs_count < max_workers
            count = jobs_count
          else
            count = max_workers
          end
        end
        
        #puts "Desired Worker Count is " + count.to_s
        #puts "Current Worker Count is " + current_workers_count.to_s
        #puts "Job count is " + jobs_count.to_s
        
        if jobs_count > 0 && current_workers_count < count
          log_and_hire(type, count)
        end
        
      end

      ##
      # This method gets invoked when a job is either "destroyed"
      # or "updated, unless the job didn't fail"
      #
      # If there are workers active, but there are no more pending jobs,
      # then fire all the workers or set to the minimum_workers
      #
      # @return [Boolean] if the workers have been fired
      def fire(type)
        if jobs(type) == 0 and workers(type) > min_workers
          Logger.message("All queued jobs in #{type} have been processed. " + (min_workers > 0 ? "Setting workers to #{min_workers}." : "Firing all workers."))
          workers(type, min_workers)
          
          # Spawn jobs to handle workers in the next lower priority level that has atleast one job in the queue
          lower_order_jobs = job_types_of_lower_order(type)

          if lower_order_jobs.present? and jobs_of_current_order(type) == 0
            lower_order_jobs.each do |job|
              if workers(job.queue) == 0
                hire(job.queue)
                HireFire::Logger.message "Starting process of type #{job.queue}"
              end
            end
          end
          
          return true
        end
        return false
      end
      
      private

      ##
      # Helper method for hire that logs the hiring of more workers, then hires those workers.
      #
      # @return [nil]
      def log_and_hire(type, amount)
        Logger.message("Hiring more #{type} workers so we have #{amount} in total.")
        workers(type, amount)
      end

      ##
      # Wrapper method for HireFire.configuration
      # Returns the max amount of workers that may run concurrently
      #
      # @return [Fixnum] the max amount of workers that are allowed to run concurrently
      def max_workers
        HireFire.configuration.max_workers
      end

      ##
      # Wrapper method for HireFire.configuration
      # Returns the min amount of workers that should always be running
      #
      # @return [Fixnum] the min amount of workers that should always be running
      def min_workers
        HireFire.configuration.min_workers
      end

      ##
      # Wrapper method for HireFire.configuration
      # Returns the job/worker ratio array
      #
      # @return [Array] the array of hashes containing the job/worker ratio
      def ratio
        HireFire.configuration.job_worker_ratio
      end

      ##
      # Wrapper method for HireFire.configuration
      # Returns the worker count for each worker type
      #
      # @return [Hash] keys are the worker types and values are the counts
      def worker_count
        HireFire.configuration.worker_count
      end
      
      ##
      # Wrapper method for HireFire.configuration
      # Returns the priority of each worker type
      #
      # @return [Hash] keys are the worker types and values are the priorities
      def worker_priority
        HireFire.configuration.worker_priority
      end
    
    end
  end
end
