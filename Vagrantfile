# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "generic/debian11"
  config.vm.synced_folder ".", "/home/vagrant/circle"


  (1..5).each do |index|
    config.vm.define "debian-v#{index}" do |machine|
      machine.vm.hostname = "debian-v#{index}"
      machine.vm.network "private_network", ip: "192.168.56.10#{index}"
      machine.vm.network "private_network", ip: "10.20.#{index}.1", virtualbox__intnet: "c1", netmask: "16"
      machine.vm.provider "virtualbox" do |v|
        v.name = "debian-v#{index}"
        v.gui = false
        v.memory = "2048"
        v.cpus = 1
      end
    end

    config.vm.provision "shell", inline: <<-SHELL
      apt update
      apt install python3-pip -y
      apt install nginx -y
      apt install mosquitto mosquitto-clients -y
      cd circle/
      pip install -r requirements.txt
    SHELL

  end
end