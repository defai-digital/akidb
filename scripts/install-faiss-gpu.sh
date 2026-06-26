#!/bin/bash
# Install FAISS GPU on NVIDIA Jetson Thor
# This script builds FAISS from source with GPU support

set -e

echo "=== Installing FAISS GPU on Jetson Thor ==="

# Install dependencies
echo "Installing dependencies..."
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    git \
    libopenblas-dev \
    liblapack-dev \
    python3-dev \
    python3-pip \
    swig

# Set CUDA environment
export CUDA_HOME=/usr/local/cuda
export PATH=$CUDA_HOME/bin:$PATH
export LD_LIBRARY_PATH=$CUDA_HOME/lib64:$LD_LIBRARY_PATH

# Check CUDA
echo "Checking CUDA..."
nvcc --version || { echo "CUDA not found. Please ensure CUDA is installed."; exit 1; }

# Create build directory
FAISS_DIR=/opt/faiss
FAISS_BUILD=/tmp/faiss-build

echo "Cloning FAISS..."
sudo mkdir -p $FAISS_DIR
sudo chown $USER:$USER $FAISS_DIR

rm -rf $FAISS_BUILD
git clone --depth 1 --branch v1.8.0 https://github.com/facebookresearch/faiss.git $FAISS_BUILD

cd $FAISS_BUILD

# Create build directory
mkdir -p build && cd build

# Configure with GPU support
echo "Configuring FAISS with GPU support..."
cmake .. \
    -DFAISS_ENABLE_GPU=ON \
    -DFAISS_ENABLE_PYTHON=OFF \
    -DBUILD_TESTING=OFF \
    -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CUDA_ARCHITECTURES="87" \
    -DCMAKE_INSTALL_PREFIX=$FAISS_DIR \
    -DCUDAToolkit_ROOT=$CUDA_HOME

# Build (use fewer jobs to avoid OOM)
echo "Building FAISS (this may take 15-30 minutes)..."
make -j4

# Install
echo "Installing FAISS..."
sudo make install

# Create pkg-config file
sudo mkdir -p $FAISS_DIR/lib/pkgconfig
sudo tee $FAISS_DIR/lib/pkgconfig/faiss.pc > /dev/null << 'EOF'
prefix=/opt/faiss
exec_prefix=${prefix}
libdir=${exec_prefix}/lib
includedir=${prefix}/include

Name: faiss
Description: Facebook AI Similarity Search
Version: 1.8.0
Libs: -L${libdir} -lfaiss
Cflags: -I${includedir}
EOF

# Update library cache
echo "$FAISS_DIR/lib" | sudo tee /etc/ld.so.conf.d/faiss.conf
sudo ldconfig

# Verify installation
echo "Verifying installation..."
ls -la $FAISS_DIR/lib/libfaiss*
ls -la $FAISS_DIR/include/faiss/

echo ""
echo "=== FAISS GPU installation complete! ==="
echo "FAISS installed to: $FAISS_DIR"
echo ""
echo "Add to your environment:"
echo "  export FAISS_PATH=$FAISS_DIR"
echo "  export LD_LIBRARY_PATH=$FAISS_DIR/lib:\$LD_LIBRARY_PATH"
