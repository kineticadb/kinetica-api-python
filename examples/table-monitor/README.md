# Table Monitor Sample  

This is a simple example to show how to use Kinetica's table monitor functionality which allows for remote processes to subscribe to insertion events on a provided table. 

# Setup
### Install needed packages
    pip install zmq gpudb 

### Configure Test Assets

 - driver.py - generates dummy data, edit to point to your kinetica host.
 - tmonitor.py - creates a monitor and subscribes to its topic, edit to point to your kinetica host

# Run 

 ### Start the data generator

    python driver.py

### Start the table monitor (in separate shell)

    python tmonitor.py

 

