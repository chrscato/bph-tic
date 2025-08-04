# Complete VM Deployment Script

## 🚀 **One-Command Deployment**

This script automates the complete deployment process from your local machine to your 32GB VM:

1. **Git Push** - Pushes your local changes to the repository
2. **SSH Connection** - Connects to your VM
3. **Clean Redeploy** - Stops, cleans, and redeploys everything
4. **Monitoring** - Shows deployment status and logs

## 📋 **Setup Requirements**

### **1. Install sshpass (Required)**
```bash
# Ubuntu/Debian
sudo apt-get install sshpass

# macOS
brew install sshpass

# CentOS/RHEL
sudo yum install sshpass
```

### **2. Update Your .env File**
Add these lines to your `.env` file:
```bash
# VM Connection Details
VM_IP=your_vm_ip_address
VM_PASSWORD=your_vm_password

# Existing S3 Configuration
S3_BUCKET=commercial-rates
S3_PREFIX=tic-mrf-data/
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-2
```

### **3. Make Script Executable**
```bash
chmod +x deploy_to_vm.sh
```

## 🎯 **Usage**

### **Simple Deployment**
```bash
./deploy_to_vm.sh
```

### **What the Script Does**

1. **Checks Repository**
   - Verifies you're in the correct directory
   - Checks for uncommitted changes
   - Offers to commit changes if needed

2. **Git Operations**
   - Pushes to `origin/master`
   - Handles branch conflicts

3. **VM Deployment**
   - Connects to VM via SSH
   - Stops current deployment
   - Cleans Docker containers/images
   - Pulls latest code from git
   - Fixes script permissions
   - Starts new deployment
   - Verifies deployment success

4. **Monitoring**
   - Shows recent logs
   - Displays container status
   - Provides monitoring commands

## 📊 **Example Output**

```
==========================================
TIC MRF Pipeline - Complete VM Deployment
==========================================
[2025-01-03 10:30:15] ✓ Repository directory confirmed
[2025-01-03 10:30:15] Checking git status...
[2025-01-03 10:30:16] ✓ No uncommitted changes
[2025-01-03 10:30:16] Pushing to git repository...
[2025-01-03 10:30:18] ✓ Successfully pushed to origin/master
[2025-01-03 10:30:18] Connecting to VM and deploying...
[2025-01-03 10:30:20] Using sshpass for password authentication
==========================================
TIC MRF Pipeline VM Deployment
==========================================
[2025-01-03 10:30:25] Stopping current deployment...
[2025-01-03 10:30:30] Cleaning Docker containers and images...
[2025-01-03 10:30:35] Pulling latest changes from git...
[2025-01-03 10:30:40] Fixing script permissions...
[2025-01-03 10:30:45] Starting deployment...
[2025-01-03 10:30:55] ✓ Deployment successful
[2025-01-03 10:31:00] ✓ VM deployment completed successfully
==========================================
✓ Deployment completed successfully!
==========================================

To monitor the deployment:
  ssh root@your_vm_ip
  cd ~/bph-tic
  ./deploy.sh logs

To check S3 uploads:
  ./deploy.sh logs | grep -i s3
```

## 🔧 **Troubleshooting**

### **sshpass Not Found**
```bash
# Install sshpass first
sudo apt-get install sshpass  # Ubuntu/Debian
brew install sshpass          # macOS
```

### **Permission Denied**
```bash
# Make script executable
chmod +x deploy_to_vm.sh
```

### **VM Connection Failed**
- Check your `.env` file has correct `VM_IP` and `VM_PASSWORD`
- Verify VM is running and accessible
- Test SSH connection manually: `ssh root@your_vm_ip`

### **Git Push Failed**
- Ensure you have write access to the repository
- Check your git credentials are configured
- Verify you're on the correct branch

## 📈 **Monitoring After Deployment**

### **Check Pipeline Status**
```bash
ssh root@your_vm_ip
cd ~/bph-tic
./deploy.sh logs
```

### **Monitor S3 Uploads**
```bash
./deploy.sh logs | grep -i s3
```

### **Check Container Status**
```bash
docker-compose ps
```

### **View Recent Logs**
```bash
./deploy.sh logs --tail=50
```

## 🎯 **What Gets Deployed**

- **Latest Code**: Pulls from `origin/master`
- **Docker Images**: Rebuilds with latest code
- **Configuration**: Uses your updated `production_config.yaml`
- **S3 Settings**: Uses your `.env` configuration
- **Timeout Protection**: Includes the new timeout fixes

## ⚡ **Performance Optimizations**

The deployment includes:
- **Memory Management**: 24GB threshold for 32GB VM
- **Batch Processing**: 25K records per batch
- **Parallel Workers**: 6 concurrent processes
- **S3 Uploads**: Every 100K records
- **Timeout Protection**: 5-minute timeout per file

## 🔄 **Workflow**

1. **Make Changes** locally
2. **Run Script**: `./deploy_to_vm.sh`
3. **Monitor**: Check logs for progress
4. **Verify**: Confirm S3 uploads are working

That's it! One command handles everything from local development to production deployment. 🚀 