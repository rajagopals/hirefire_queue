# encoding: utf-8

require 'heroku'

module HireFire
  module Environment
    class Heroku < Base

      private

      ##
      # Either retrieves the amount of currently running workers,
      # or set the amount of workers to a specific amount by providing a value
      #
      # @overload workers(amount = nil)
      #   @param [Fixnum] amount will tell heroku to run N workers
      #   @return [nil]
      # @overload workers(amount = nil)
      #   @param [nil] amount
      #   @return [Fixnum] will request the amount of currently running workers from Heroku
      def workers(type, amount = nil)

        if amount.nil?
          # return client.info(ENV['APP_NAME'])[:workers].to_i
          return client.ps(ENV['APP_NAME']).select {|p| p['process'].index(type.to_s) != nil}.length
        end

        # client.set_workers(ENV['APP_NAME'], amount)
        return client.ps_scale(ENV['APP_NAME'], {"type" => type.to_s, "qty" => amount})

      rescue RestClient::Exception
        HireFire::Logger.message("Worker query request failed with #{ $!.class.name } #{ $!.message }")
        nil
      end

      ##
      # @return [Heroku::Client] instance of the heroku client
      def client
        @client ||= ::Heroku::Client.new(
          ENV['HIREFIRE_EMAIL'], ENV['HIREFIRE_PASSWORD']
        )
      end

    end
  end
end
