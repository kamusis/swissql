#!/bin/bash
cd /Users/kamus/CascadeProjects/swissql/swissql-cli

# Test the remove command with --like flag
echo "Testing: connmgr remove db__ --like"
echo "db__" | ./swissql <<'EOF'
connmgr remove db__ --like --force
exit
EOF
