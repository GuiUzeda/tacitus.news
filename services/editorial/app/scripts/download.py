from sentence_transformers import SentenceTransformer

if __name__ == "__main__":
    model_name = "nomic-ai/nomic-embed-text-v1.5"
    print(f"Downloading model: {model_name}")
    SentenceTransformer(model_name, trust_remote_code=True, device="cpu")
    print("Download complete.")
