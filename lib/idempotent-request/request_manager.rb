module IdempotentRequest
  class RequestManager
    attr_reader :request, :storage

    def initialize(request, config)
      @request = request
      @storage = config.fetch(:storage)
      @context = config[:context]
      @callback = config[:callback]
    end

    def lock
      storage.lock(key)
    end

    def unlock
      storage.unlock(key)
    end

    def read
      status, headers, response = parse_data(storage.read(key)).values

      return unless status
      run_callback(:detected, key: request.key)
      [status, headers, response]
    end

    def write(*data)
      status, headers, response = data
      response = response.body if response.respond_to?(:body)
      storage.write(key, payload(status, headers, response))
      data
    end

    private

    def parse_data(data)
      return {} if data.to_s.empty?

      MessagePack.unpack(data)
    end

    def payload(status, headers, response)
      MessagePack.pack(
        status: status,
        headers: headers.to_h,
        response: Array(response)
      )
    end

    def run_callback(action, args)
      return unless @callback

      @callback.new(request).send(action, args)
    end

    def context
      @context.new(request).context if @context
    end

    def key
      [context, request.key].compact.join('-')
    end
  end
end
