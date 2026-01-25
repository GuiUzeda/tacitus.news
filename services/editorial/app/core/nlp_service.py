from loguru import logger
from sentence_transformers import SentenceTransformer


class NLPService:
    _instance = None

    STOP_PHRASES = [
        "apoie o jornalismo",
        "assine a edição",
        "assine agora",
        "faça parte da nossa comunidade",
        "receba as notícias",
        "receba as principais notícias",
        "siga-nos no",
        "siga a gente",
        "clique aqui para",
        "leia mais em:",
        "leia também:",
        "copyright ©",
        "todos os direitos reservados",
        "entre no canal do whatsapp",
        "participe do grupo",
        "conteúdo exclusivo para assinantes",
        "fale com o colunista",
        "newsletter",
        "inscreva-se",
        "baixe o app",
        "google news",
        "redação:",
        "colaboração para o",
        "veja também",
        "o post apareceu primeiro em",
    ]

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(NLPService, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        logger.info("Loading AI Models (NLPService)...")
        # 1. Embedding Model
        self.embedder = SentenceTransformer(
            "nomic-ai/nomic-embed-text-v1.5", trust_remote_code=True, device="cpu"
        )
        # Warmup to initialize lazy buffers (Rotary Embeddings) before multi-threaded use
        self.embedder.encode("warmup " * 1000)

        # 2. NER Model (SpaCy)

        logger.info("NLPService Initialized.")

    def clean_text_for_embedding(self, text: str) -> str:
        """
        Aggressively strips footer boilerplate to prevent vector contamination.
        """
        if not text:
            return ""

        lines = text.split("\n")
        clean_lines = []

        # Heuristic: If we hit a stop phrase, we assume everything after is junk.
        for line in lines:
            lower_line = line.lower().strip()

            # Check for exact stop phrase matches
            if any(phrase in lower_line for phrase in self.STOP_PHRASES):
                # Extra check: Don't stop if the line is very long (it might be a narrative sentence containing a common word)
                if len(line) < 150:
                    break

            clean_lines.append(line)

        cleaned_text = "\n".join(clean_lines).strip()

        # Safety Fallback: If we stripped too much (e.g., empty string), use the original first 1000 chars
        if len(cleaned_text) < 50 and len(text) > 200:
            logger.warning(
                "Cleaning removed almost all text. Reverting to head of original."
            )
            return text[:2000]

        return cleaned_text

    def calculate_vector(self, text: str) -> list[float]:
        """
        Generates vector from CLEANED and TRUNCATED text.
        """
        if not text or len(text) < 10:
            return [0.0] * 768

        # 1. Clean
        clean_txt = self.clean_text_for_embedding(text)

        # 2. Truncate (Nomic allows 8192, but for clustering, the lead is key)
        truncated_txt = clean_txt[:20000]

        prefix = "search_document: "
        try:
            return self.embedder.encode(prefix + truncated_txt).tolist()
        except Exception as e:
            logger.error(f"Vectorization failed: {e}")
            return [0.0] * 768
