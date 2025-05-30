import requests
import base64

# Thay bằng thông tin của bạn
client_id = 'fe50ea9be366451b88f9697dfcd9f28f'
client_secret = '8cf41a34aaeb41098e3bde0ab1e9a38f'

# Encode client_id:client_secret to base64
auth_string = f"{client_id}:{client_secret}"
auth_bytes = auth_string.encode('utf-8')
auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')

# Headers and data
headers = {
    "Authorization": f"Basic {auth_base64}",
    "Content-Type": "application/x-www-form-urlencoded"
}
data = {
    "grant_type": "client_credentials"
}

# Make the request
response = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=data)

# Parse response
if response.status_code == 200:
    token_info = response.json()
    access_token = token_info['access_token']
    expires_in = token_info['expires_in']

    print(f"✅ Bearer Token: {access_token}")
    print(f"⏳ Expires in: {expires_in} seconds")
else:
    print("❌ Failed to get token")
    print(response.status_code, response.text)
