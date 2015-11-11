# IPOP Controllers

### Prerequisites

Obtain **IPOP-Controllers** by downloading the latest archive from the releases or by cloning this repository:
```git clone https://github.com/ipop-project/controllers.git```

Obtain **IPOP-Tincan** by downloading the latest archive from the releases or by building from source [2].


### Usage

#### Running SocialVPN

1. Change current directory to ```controller```.

    ```cd controllers/controller/```

2. Modify ```svpn-config.json``` to customize the controller's configuration [1].

    ```nano modules/svpn-config.json```

3. Run IPOP-Tincan.

    ```sudo ./ipop-tincan-x86_64 &> tin.log &```

4. Run SocialVPN Controller.

	Make sure you return to the <path to _controllers_>/controllers/ directory.
	```
	cd ..
	python -m controller.Controller -c modules/svpn-config.json &> log.txt &
	```

5. Check status.

    ```echo -e '\x02\x01{"m":"get_state"}' | netcat -q 1 -u 127.0.0.1 5800```

#### Running GroupVPN

1. Change current directory to ```controller```.

    ```cd controllers/controller/```

2. Modify ```gvpn-config.json``` to customize the controller's configuration [1].

    ```nano modules/gvpn-config.json```

3. Run IPOP-Tincan.

    ```sudo ./ipop-tincan-x86_64 &> tin.log &```

4. Run GroupVPN Controller.

	Make sure you return to the <path to _controllers_>/controllers/ directory.
	```
	cd ..
	python -m controller.Controller -c modules/gvpn-config.json &> log.txt &
	```

5. Check status.

    ```echo -e '\x02\x01{"m":"get_state"}' | netcat -q 1 -u 127.0.0.1 5800```

#### Killing IPOP

The following commands will search the PID of IPOP-Tincan and IPOP-Controller and dispatch a kill signal.
```
ps aux | grep -v grep | grep ipop-tincan-x86_64 | awk '{print $2}' | xargs sudo kill -9
ps aux | grep -v grep | grep controller.Controller.py | awk '{print $2}' | xargs sudo kill -9
```

### Notes

[1] See https://github.com/ipop-project/documentation/wiki/Configuration for a detailed description of options for controller configuartion.

[2] See https://github.com/ipop-project/documentation/wiki under _Building the code_ for guides on building IPOP-Tincan from source.
