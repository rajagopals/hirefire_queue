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

        def jobs_of_higher_order(type)
          priority = Delayed::Job.environment.worker_priority[type.to_sym].to_i     
          Delayed::Job.where(['priority < ?', priority]).count
        end
        
        def jobs_of_current_order(type)
          priority = Delayed::Job.environment.worker_priority[type.to_sym].to_i     
          Delayed::Job.where(['priority = ?', priority]).count        
        end
        
        def jobs_of_lower_order(type)
          priority = Delayed::Job.environment.worker_priority[type.to_sym].to_i     
          Delayed::Job.where(['priority > ?', priority]).count        
        end        

        def job_types_of_lower_order(type)
          priority = Delayed::Job.environment.worker_priority[type.to_sym].to_i
          val = []
          loop do
            priority = priority + 1
            val = Delayed::Job.select('`delayed_jobs`.`queue`').order('`delayed_jobs`.`run_at`').group('`delayed_jobs`.`queue`').where(['priority = ?', priority]).all
            break if val.present? or Delayed::Job.environment.worker_priority.values.max <= priority
          end
          val
        end

      end
    end
  end
end
