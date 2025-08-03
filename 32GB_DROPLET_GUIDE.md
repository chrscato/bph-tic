# 32GB RAM Droplet Deployment Guide

## 🚀 Optimized for Your 32GB RAM, 4 CPU, 100GB NVMe SSD Droplet

### **Droplet Specifications**
- **RAM**: 32GB
- **CPU**: 4 vCPUs
- **Storage**: 100GB NVMe SSD
- **Transfer**: 6TB
- **Cost**: ~$0.24/hour (~$172/month if running 24/7)

## 🎯 Quick Start

### **1. Initial Setup**
```bash
# Connect to your droplet
ssh root@your-droplet-ip

# Clone repository and setup
git clone <your-repo-url>
cd bph-tic
chmod +x quick_setup.sh
./quick_setup.sh setup
```

### **2. Configure Environment**
```bash
# Edit environment file
nano .env

# Update these key settings:
S3_BUCKET=your-s3-bucket-name
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
```

### **3. Start Processing**
```bash
# For cost optimization (12 hours max)
./deploy.sh quick_run

# For maximum performance (uses full 32GB RAM)
./deploy.sh high_performance

# Manual control
./deploy.sh start
./deploy.sh monitor
./deploy.sh stop
```

## ⚡ Performance Optimizations

### **Memory Allocation**
- **Container Limits**: 28GB max, 16GB reserved
- **Application Threshold**: 24GB (75% of 32GB)
- **Batch Size**: 25,000 records (vs 10,000 for smaller droplets)
- **Workers**: 6 parallel workers (vs 4 for smaller droplets)

### **High-Performance Mode**
```bash
./deploy.sh high_performance
```
**Settings:**
- Batch size: 50,000 records
- Max workers: 8
- Memory threshold: 28GB
- Full 32GB RAM utilization

## 💰 Cost Management

### **Cost Breakdown**
- **Hourly**: $0.24
- **Daily**: $5.76
- **Weekly**: $40.32
- **Monthly (24/7)**: $172.80

### **Cost Optimization Strategies**

#### **1. Time-Limited Processing**
```bash
# Auto-stops after 12 hours
./deploy.sh quick_run
```

#### **2. Idle Detection**
- Stops after 45 minutes of inactivity
- Monitors log activity automatically

#### **3. Resource Monitoring**
```bash
# Monitor costs and resources
./monitor_resources.sh

# Continuous monitoring
./monitor_resources.sh monitor
```

#### **4. Automated Cleanup**
- Daily cleanup at 2 AM
- Weekly cost reports on Sundays
- 14-day backup retention
- 45-day log retention

## 📊 Resource Monitoring

### **Memory Usage**
- **Alert Threshold**: 24GB (75% of 32GB)
- **Optimal Range**: 16-24GB
- **Peak Usage**: Up to 28GB in high-performance mode

### **CPU Usage**
- **Available**: 4 vCPUs
- **Optimal**: 2-3 vCPUs for processing
- **Reserved**: 2 vCPUs for system

### **Storage Usage**
- **Total**: 100GB NVMe SSD
- **Application Data**: Up to 80GB
- **System Reserve**: 20GB
- **Alert Threshold**: 85% usage

## 🔧 Configuration Options

### **Environment Variables (env.production)**
```bash
# Processing Configuration (32GB Optimized)
BATCH_SIZE=25000
MAX_WORKERS=6
MEMORY_THRESHOLD_MB=24576
MAX_RECORDS_PER_FILE=500000
MAX_FILES_PER_PAYER=200

# Cost Optimization
MAX_PROCESSING_TIME_HOURS=12
MAX_MEMORY_USAGE_MB=28672

# High-Performance Settings
ENABLE_PARALLEL_PROCESSING=true
ENABLE_MEMORY_MAPPING=true
ENABLE_COMPRESSION=true
COMPRESSION_LEVEL=6
```

### **Docker Resource Limits**
```yaml
deploy:
  resources:
    limits:
      memory: 28G
      cpus: '4.0'
    reservations:
      memory: 16G
      cpus: '2.0'
```

## 🎯 Processing Scenarios

### **Scenario 1: Cost-Conscious Processing**
```bash
# Start for 12 hours, then auto-stop
./deploy.sh quick_run

# Monitor progress
./deploy.sh monitor

# Estimated cost: $2.88 for 12 hours
```

### **Scenario 2: Maximum Throughput**
```bash
# Use full 32GB RAM for maximum speed
./deploy.sh high_performance

# Monitor high-performance mode
./monitor_resources.sh monitor

# Estimated cost: $0.24/hour
```

### **Scenario 3: Batch Processing**
```bash
# Start processing
./deploy.sh start

# Monitor until complete
./deploy.sh monitor

# Stop when done
./deploy.sh stop

# Estimated cost: $0.24/hour while running
```

## 📈 Performance Expectations

### **Processing Speed**
- **Standard Mode**: ~50,000 records/hour
- **High-Performance Mode**: ~100,000 records/hour
- **Memory Usage**: 16-24GB typical, up to 28GB peak
- **CPU Usage**: 60-80% during processing

### **Storage Throughput**
- **NVMe SSD**: High-speed storage for data processing
- **Data Processing**: Up to 80GB of processed data
- **Backup Storage**: 14-day retention with compression

### **Network Performance**
- **6TB Transfer**: Ample bandwidth for data processing
- **S3 Upload**: Optimized for large batch uploads
- **Download Speed**: Fast data retrieval from MRF sources

## 🔍 Monitoring Commands

### **Resource Monitoring**
```bash
# Check current status
./deploy.sh status

# Monitor resources and costs
./monitor_resources.sh

# View live logs
./deploy.sh logs

# Check container health
docker ps
docker stats
```

### **Cost Tracking**
```bash
# Real-time cost estimation
./monitor_resources.sh report

# Weekly cost reports (automated)
cat logs/weekly_cost_report.log

# Monthly cost projection
echo "Monthly cost if running 24/7: $172.80"
```

## 🛠️ Troubleshooting

### **High Memory Usage**
```bash
# Check memory usage
free -h

# Reduce memory threshold if needed
export MEMORY_THRESHOLD_MB=20480  # 20GB

# Restart with lower settings
./deploy.sh restart
```

### **Slow Processing**
```bash
# Enable high-performance mode
./deploy.sh high_performance

# Or increase workers manually
export MAX_WORKERS=8
./deploy.sh restart
```

### **Storage Issues**
```bash
# Check disk usage
df -h

# Clean up old data
./deploy.sh cleanup

# Check backup size
du -sh backups/
```

## 💡 Best Practices

### **1. Start Small, Scale Up**
- Begin with `quick_run` to test processing
- Use `high_performance` for large datasets
- Monitor costs with `monitor_resources.sh`

### **2. Optimize for Your Data**
- Adjust `BATCH_SIZE` based on record size
- Modify `MAX_WORKERS` based on CPU usage
- Tune `MEMORY_THRESHOLD_MB` based on memory usage

### **3. Cost Management**
- Use `quick_run` for time-limited processing
- Stop droplets when not in use
- Monitor costs weekly
- Clean up old data regularly

### **4. Performance Monitoring**
- Check resource usage before starting large jobs
- Monitor memory usage during processing
- Track processing speed and adjust settings
- Use logs to identify bottlenecks

## 🎯 Expected Results

### **Processing Capacity**
- **Small Dataset** (< 1M records): 2-4 hours
- **Medium Dataset** (1-10M records): 4-12 hours
- **Large Dataset** (> 10M records): 12+ hours (multiple runs)

### **Cost Efficiency**
- **Optimal Usage**: $0.24/hour while processing
- **Cost Savings**: Stop when not processing
- **ROI**: Process large datasets efficiently with 32GB RAM

### **Data Quality**
- **High Throughput**: Process more data faster
- **Better Quality**: More memory for data validation
- **Reliable Processing**: Stable environment with 32GB RAM

## 🚀 Next Steps

1. **Deploy to your 32GB droplet** using the quick setup
2. **Configure AWS credentials** in the `.env` file
3. **Test with a small dataset** using `quick_run`
4. **Scale up** to `high_performance` for large datasets
5. **Monitor costs** and optimize based on your usage patterns

Your 32GB RAM droplet is perfectly sized for high-performance healthcare data processing with excellent cost efficiency when managed properly! 