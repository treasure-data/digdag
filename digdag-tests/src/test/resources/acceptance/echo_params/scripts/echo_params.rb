class EchoParams
  def echo_params
    puts 'digdag params'
    Digdag.env.params.each do |k,v|
      puts "#{k} #{v}"
    end
  end
end
