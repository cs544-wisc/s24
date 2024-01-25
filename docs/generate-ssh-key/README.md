# Generating SSH Key Pairs

To connect to a remote machine (e.g., GCP VMs) through SSH, you will need to register your public key to the remote party and start your SSH session with your private key. By default, when you start an SSH session, it will use your private key stored at `~/.ssh/id_rsa` (the corresponding public key is `~/.ssh/id_rsa.pub`). Here `~` is your home directory, that is
* `C:\Users\<username>` on Windows
* `/Users/<username>` on MacOS
* On linux the path is usually `/users/<username>` but it might differ (regardless, you should be able to find it with `pwd ~`)

If you see your key pairs, simply copy your public key (content of `~/.ssh/id_rsa.pub`) to the remote party and you should be good to go. 

If not, here is how you can generate a new key pair:
* On your local command line, run `ssh-keygen`. 
* Hit enter (use defaults) for all the prompts. 
  
This will create a RSA key pair stored in `~/.ssh/id_rsa` and `~/.ssh/id_rsa.pub`. As before, copy `~/.ssh/id_rsa.pub` to the remote party and you should be able to SSH to the remote machine. 

## Use Non-defualt Key Pairs
Suppose for some reason, you don't want to connect to the remote machine with the default key pairs. For example, if you want to use different key pairs to connect to AWS and GCP, or the remote party generates the key pairs for you and you have to use the private key they give you (this happens with AWS EC2 machines). In this case, when you start the SSH session, you need to specify which private key to use
```sh
ssh -i <path-to-your-private-key> <remote-host-address>
ssh -i <path-to-your-private-key> <username>@<remote-host-address>
...
```
When you generate the key pairs with `ssh-keygen`, instead of using defaults for all prompts, answer the prompt `Enter file in which to save the key` with the path where you would like to save your private key. You will see the corresponding public key generated under the same directory as your private key with the same name and an extension `.pub`. 
