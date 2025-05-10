#!/bin/bash

# -----------------------------
# 发布新版本脚本（Git Tag + 推送）
# 用法： ./release.sh v1.0.1
# -----------------------------

# 判断是否传入 tag 参数
if [ -z "$1" ]; then
  echo "❌ 错误：请传入版本号，例如：./release.sh v1.0.1"
  exit 1
fi

VERSION=$1

echo "🚀 开始发布版本：$VERSION"

# 1. 确保代码是最新的（可选，根据需要打开）
echo "📦 正在提交当前改动..."
git add .
git commit -m "release: $VERSION"

# 2. 创建 tag
echo "🔖 创建 Git tag：$VERSION"
git tag "$VERSION"

# 3. 推送代码
echo "⏫ 推送代码到远程仓库..."
git push origin main

# 4. 推送 tag
echo "📤 推送 tag 到远程仓库..."
git push origin "$VERSION"

echo "✅ 发布完成！你现在可以去 GitHub Actions 看自动构建啦！"