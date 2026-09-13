"""阿里云 OSS 上传/访问测试脚本。

核心方法 upload_file_and_get_url()：上传本地文件到 OSS，返回带过期时间的签名访问 URL
（可选经反代域名改写 host）。测试流程：写一个本地临时文件 -> 用该方法上传并拿到 URL ->
通过该 URL 读取核对内容一致 -> 清理测试对象和临时文件。

注意：OSS 账号/桶开启了"公共访问块"时不允许设置 public-read ACL
（AccessDenied: Put public object acl is not allowed），所以不依赖匿名读，
而是用签名 URL 访问 —— 签名基于 bucket+object，与请求用的域名无关，
经反代（只改写 Host/SNI，原样转发 path 和 query）一样能验证通过。

用法：
    pip install oss2 requests
    设置环境变量 OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET / OSS_ENDPOINT /
        OSS_BUCKET_NAME / OSS_PROXY_BASE_URL
    （或在本文件同目录放一个 oss_test_config.json，参考 oss_test_config.example.json）
    python oss_test.py
"""
import json
import os
import sys
import tempfile
import time
import uuid
from urllib.parse import urlsplit, urlunsplit

import requests

try:
    import oss2
except ImportError:
    print("缺少依赖 oss2，请先执行: pip install oss2")
    sys.exit(1)

CONFIG_KEYS = ("access_key_id", "access_key_secret", "endpoint", "bucket_name", "proxy_base_url")

DEFAULT_EXPIRE_SECONDS = 7 * 24 * 60 * 60  # 签名 URL 默认有效期：7 天


def load_config():
    config = {
        "access_key_id": os.environ.get("OSS_ACCESS_KEY_ID", ""),
        "access_key_secret": os.environ.get("OSS_ACCESS_KEY_SECRET", ""),
        "endpoint": os.environ.get("OSS_ENDPOINT", ""),
        "bucket_name": os.environ.get("OSS_BUCKET_NAME", ""),
        "proxy_base_url": os.environ.get("OSS_PROXY_BASE_URL", ""),
    }

    if not all(config[k] for k in CONFIG_KEYS):
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "oss_test_config.json")
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                file_config = json.load(f)
            for k in CONFIG_KEYS:
                if not config[k] and file_config.get(k):
                    config[k] = file_config[k]

    missing = [k for k in CONFIG_KEYS if not config[k]]
    if missing:
        print(f"缺少配置项: {', '.join(missing)}")
        print("请设置环境变量 OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET / OSS_ENDPOINT / "
              "OSS_BUCKET_NAME / OSS_PROXY_BASE_URL")
        print("或在脚本同目录创建 oss_test_config.json（参考 oss_test_config.example.json）")
        sys.exit(1)
    config["proxy_base_url"] = config["proxy_base_url"].rstrip("/")
    return config


def rehost(url, proxy_base_url):
    """把签名 URL 的 scheme+host 换成反代域名，保留 path 和签名 query 不变。"""
    parts = urlsplit(url)
    proxy_parts = urlsplit(proxy_base_url)
    return urlunsplit((proxy_parts.scheme, proxy_parts.netloc, parts.path, parts.query, ""))


def upload_file_and_get_url(bucket, local_path, object_key,
                             expires=DEFAULT_EXPIRE_SECONDS, proxy_base_url=None):
    """上传本地文件到 OSS，返回签名访问 URL。

    :param bucket: oss2.Bucket 实例
    :param local_path: 待上传的本地文件路径
    :param object_key: 上传后的 OSS 对象 key
    :param expires: 访问 URL 有效期，单位秒，默认 7 天
    :param proxy_base_url: 反代域名；传入时会把签名 URL 的域名替换为该域名
    :return: 文件访问 URL（字符串）
    """
    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"本地文件不存在: {local_path}")
    bucket.put_object_from_file(object_key, local_path)
    url = bucket.sign_url("GET", object_key, expires)
    if proxy_base_url:
        url = rehost(url, proxy_base_url)
    return url


def main():
    config = load_config()
    auth = oss2.Auth(config["access_key_id"], config["access_key_secret"])
    bucket = oss2.Bucket(auth, config["endpoint"], config["bucket_name"])

    content = f"oss upload test @ {time.strftime('%Y-%m-%d %H:%M:%S')}".encode("utf-8")
    object_key = f"oss_test/{time.strftime('%Y%m%d')}/{uuid.uuid4().hex}.txt"

    fd, local_path = tempfile.mkstemp(suffix=".txt")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(content)

        print(f"[1/3] 上传本地文件并生成访问 URL: {local_path} -> {object_key}")
        url = upload_file_and_get_url(
            bucket, local_path, object_key,
            expires=60,  # 测试用短有效期即可，生产调用不传则默认 7 天
            proxy_base_url=config["proxy_base_url"],
        )
        print(f"访问 URL: {url}")

        print("[2/3] 通过访问 URL 读取并核对内容")
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            print(f"校验失败：请求返回 status={resp.status_code}，body={resp.text[:300]!r}")
            print("请检查 Caddy 的 Host 头改写 / tls_server_name 是否与 OSS endpoint 一致")
            sys.exit(1)
        if resp.content != content:
            print("校验失败：读取内容与上传内容不一致")
            sys.exit(1)
        print("读取成功，内容一致")
    finally:
        print("[3/3] 清理测试对象和本地临时文件")
        bucket.delete_object(object_key)
        os.remove(local_path)
        print("清理完成")

    print("\nOSS 上传测试通过")


if __name__ == "__main__":
    main()
