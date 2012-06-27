# encoding: utf-8

module HireFire
  module Backend
    module DelayedJob
      module ActiveRecord

        ##
        # Counts the amount of queued jobs in the database,
        # failed jobs are excluded from the sum
        #
        # @return [Fixnum] the amount of pending jobs
        def jobs(type)
          ::Delayed::Job.
          where(:failed_at => nil).
          where('run_at <= ?', Time.now).
          where(:queue => type.to_s).count
        end

        ##
        # Counts the amount of jobs that are locked by a worker
        # There is no other performant way to determine the amount
        # of workers there currently are
        #
        # @return [Fixnum] the amount of (assumably working) workers
        def working(type)
          ::Delayed::Job.
          where('locked_by IS NOT NULL').
          where(:queue => type.to_s).count
        end

        ##
        # Counts the amount of jobs that are of higher priority
        # than the priority of the worker type passed as argument
        #
        # @return [Fixnum] the amount of higher priority workers
        def jobs_of_higher_order(type)
          priority = HireFire.configuration.worker_priority[type.to_sym].to_i     
          Delayed::Job.where(['priority < ?', priority]).count
        end
                
        ##
        # Counts the amount of jobs that are of the same priority
        # as the priority of the worker type passed as argument
        #
        # @return [Fixnum] the amount of workers with the same priority
        def jobs_of_current_order(type)
          priority = HireFire.configuration.worker_priority[type.to_sym].to_i     
          Delayed::Job.where(['priority = ?', priority]).count        
        end
        
        ##
        # Counts the amount of jobs that are of lower priority
        # than the priority of the worker type passed as argument
        #
        # @return [Fixnum] the amount of lower priority workers
        def jobs_of_lower_order(type)
          priority = HireFire.configuration.worker_priority[type.to_sym].to_i     
          Delayed::Job.where(['priority > ?', priority]).count        
        end        

        ##
        # Returns an array with the names of all workers of priority 'n' levels lower
        # than the worker type passed as argument. n is the highest lower-priority level 
        # that has atleast one job in the queue.
        #
        # @return [Array] All job types of a particular priority value 
        # (that is lower than the priority of the type passed as argument)
        def job_types_of_lower_order(type)
          priority = HireFire.configuration.worker_priority[type.to_sym].to_i
          max_priority = HireFire.configuration.worker_priority.values.max
          val = []
          loop do
            priority = priority + 1
            val = Delayed::Job.select('`delayed_jobs`.`queue`').order('`delayed_jobs`.`run_at`').group('`delayed_jobs`.`queue`').where(['priority = ?', priority]).all
            break if val.present? or priority >= max_priority
          end
          val
        end

      end
    end
  end
end
