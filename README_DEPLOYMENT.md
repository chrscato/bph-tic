# TIC MRF Scraper - Digital Ocean Deployment Guide

This guide will help you deploy the TIC MRF Scraper on a high-memory Digital Ocean droplet with cost optimization strategies.

## Prerequisites

### Digital Ocean Droplet Requirements
- **Size**: At least 8GB RAM, 4 vCPUs (recommended: 16GB RAM, 8 vCPUs)
- **Storage**: At least 100GB SSD
- **OS**: Ubuntu 20.04 LTS or newer
- **Region**: Choose closest to your data sources

### Software Requirements
- Docker
- Docker Compose
- Git

## Quick Setup

### 1. Connect to Your Droplet
```bash
ssh root@your-droplet-ip
```

### 2. Install Docker and Docker Compose
```bash
# Update system
apt update && apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Install Docker Compose
curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Add user to docker group
usermod -aG docker $USER
```

### 3. Clone and Setup the Repository
```bash
# Clone the repository
git clone <your-repo-url>
cd bph-tic

# Make deployment script executable
chmod +x deploy.sh

# Copy environment template
cp env.production .env
```

### 4. Configure Environment Variables
Edit the `.env` file with your specific configuration:

```bash
nano .env
```

**Required Configuration:**
```bash
# AWS S3 Configuration (REQUIRED)
S3_BUCKET=your-s3-bucket-name
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_DEFAULT_REGION=us-east-1

# Processing Configuration (OPTIONAL - adjust based on your droplet size)
BATCH_SIZE=10000
MAX_WORKERS=4
MEMORY_THRESHOLD_MB=6144
```

### 5. Deploy the Application

#### Option A: Quick Start (Recommended for Cost Optimization)
```bash
# Start for 8 hours then auto-stop
./deploy.sh quick_run
```

#### Option B: Manual Control
```bash
# Start the application
./deploy.sh start

# Check status
./deploy.sh status

# View logs
./deploy.sh logs

# Stop when done
./deploy.sh stop
```

## Cost Optimization Strategies

### 1. Time-Limited Processing
Use the `quick_run` command to automatically stop after 8 hours:
```bash
./deploy.sh quick_run
```

### 2. Manual Start/Stop
Start only when you need to process data:
```bash
# Start processing
./deploy.sh start

# Monitor progress
./deploy.sh monitor

# Stop when complete
./deploy.sh stop
```

### 3. Resource Monitoring
Monitor resource usage to optimize costs:
```bash
# Check resource usage
./deploy.sh monitor

# View container stats
docker stats
```

### 4. Data Management
Clean up old data to save storage costs:
```bash
# Clean up old containers and data
./deploy.sh cleanup
```

## Configuration Options

### Memory Management
Adjust these settings based on your droplet size:

```bash
# For 8GB RAM droplet
MEMORY_THRESHOLD_MB=6144
MAX_WORKERS=4

# For 16GB RAM droplet
MEMORY_THRESHOLD_MB=12288
MAX_WORKERS=8
```

### Processing Limits
Control processing to manage costs:

```bash
# Limit processing time
MAX_PROCESSING_TIME_HOURS=8

# Limit records per file
MAX_RECORDS_PER_FILE=100000

# Limit files per payer
MAX_FILES_PER_PAYER=50
```

## Monitoring and Troubleshooting

### View Application Status
```bash
./deploy.sh status
```

### View Live Logs
```bash
./deploy.sh logs
```

### Monitor Resources
```bash
./deploy.sh monitor
```

### Check Container Health
```bash
docker ps
docker logs tic-mrf-scraper
```

## Data Backup and Recovery

### Automatic Backups
The deployment script automatically creates backups when stopping:
```bash
# Backups are stored in ./backups/
ls -la backups/
```

### Manual Backup
```bash
# Create manual backup
tar -czf backup_$(date +%Y%m%d_%H%M%S).tar.gz data/
```

### Restore from Backup
```bash
# Stop application
./deploy.sh stop

# Restore data
tar -xzf backup_YYYYMMDD_HHMMSS.tar.gz

# Restart application
./deploy.sh start
```

## Advanced Configuration

### Custom Processing Configuration
Edit `production_config.yaml` for specific processing needs:

```yaml
processing:
  batch_size: 10000
  parallel_workers: 4
  max_files_per_payer: 50
  max_records_per_file: 100000
```

### S3 Configuration
Configure S3 for data storage:

```bash
# In .env file
S3_BUCKET=your-bucket-name
S3_PREFIX=tic-mrf-data/providers
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
```

### Logging Configuration
Adjust logging levels:

```bash
# In .env file
LOG_LEVEL=INFO  # or DEBUG, WARNING, ERROR
LOG_FILE=/app/logs/etl_pipeline.log
```

## Security Considerations

### Environment Variables
- Never commit `.env` files to version control
- Use strong AWS credentials
- Rotate credentials regularly

### Container Security
- Application runs as non-root user
- Health checks are enabled
- Resource limits are configured

### Network Security
- Configure firewall rules on Digital Ocean
- Use SSH keys for droplet access
- Consider VPN for secure access

## Performance Optimization

### Memory Optimization
```bash
# Monitor memory usage
./deploy.sh monitor

# Adjust memory threshold if needed
MEMORY_THRESHOLD_MB=6144  # 75% of 8GB
```

### CPU Optimization
```bash
# Adjust worker count based on CPU cores
MAX_WORKERS=4  # For 4 vCPU droplet
```

### Storage Optimization
```bash
# Clean up old data
./deploy.sh cleanup

# Monitor disk usage
df -h
```

## Troubleshooting

### Common Issues

#### Container Won't Start
```bash
# Check logs
docker-compose logs

# Check environment file
cat .env

# Rebuild container
docker-compose build --no-cache
```

#### Memory Issues
```bash
# Check memory usage
free -h

# Reduce memory threshold
MEMORY_THRESHOLD_MB=4096
```

#### S3 Upload Issues
```bash
# Check AWS credentials
aws sts get-caller-identity

# Test S3 access
aws s3 ls s3://your-bucket-name
```

#### Processing Too Slow
```bash
# Increase workers
MAX_WORKERS=8

# Increase batch size
BATCH_SIZE=20000
```

## Cost Estimation

### Digital Ocean Droplet Costs (Monthly)
- 8GB RAM, 4 vCPU: ~$48/month
- 16GB RAM, 8 vCPU: ~$96/month

### S3 Storage Costs
- Standard storage: ~$0.023/GB/month
- Data transfer: ~$0.09/GB (outbound)

### Optimization Tips
1. Use `quick_run` for time-limited processing
2. Stop droplets when not in use
3. Clean up old data regularly
4. Monitor resource usage
5. Use appropriate droplet size for your workload

## Support

For issues or questions:
1. Check logs: `./deploy.sh logs`
2. Monitor resources: `./deploy.sh monitor`
3. Review configuration files
4. Check Digital Ocean droplet status

## Next Steps

1. Configure your AWS S3 credentials
2. Adjust processing parameters for your data volume
3. Set up monitoring and alerts
4. Test with a small dataset first
5. Scale up based on performance and cost requirements 