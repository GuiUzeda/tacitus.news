import enum
from dataclasses import dataclass
from typing import List, Optional

from app.config import Settings

# Project Imports
from app.core.llm_parser import CloudNewsAnalyzer, LLMNewsOutputSchema
from loguru import logger


class AnalystStatus(str, enum.Enum):
    SUCCESS = "SUCCESS"  # Valid article, enriched
    IRRELEVANT = (
        "IRRELEVANT"  # Valid processing, but article is garbage/ad -> Archive it
    )
    ERROR = "ERROR"  # LLM/Network failure -> Retry


@dataclass
class AnalystResult:
    status: AnalystStatus
    data: Optional[LLMNewsOutputSchema] = None
    reason: str = ""


class ContentAnalystDomain:
    def __init__(self):
        self.settings = Settings()
        self.llm = CloudNewsAnalyzer()

    async def analyze_batch(self, texts: List[str]) -> List[AnalystResult]:
        """
        Pure Logic: Sends text batch to LLM, returns structured status.
        """
        if not texts:
            return []

        logger.info(f"🧠 Analyst: Running LLM on {len(texts)} articles...")

        try:
            # The LLM Service returns a List[LLMNewsOutputSchema]
            outputs = await self.llm.analyze_articles_batch(texts)

            results = []
            for output in outputs:
                if output.status == "valid":
                    results.append(
                        AnalystResult(AnalystStatus.SUCCESS, output, "LLM Success")
                    )
                elif output.status == "irrelevant":
                    results.append(
                        AnalystResult(
                            AnalystStatus.IRRELEVANT,
                            None,
                            f"Irrelevant: {output.error_message}",
                        )
                    )
                else:
                    # 'error' status from LLM Schema means parsing failed or model refused
                    results.append(
                        AnalystResult(
                            AnalystStatus.ERROR,
                            None,
                            f"LLM Processing Failed: {output.error_message}",
                        )
                    )

            return results

        except Exception as e:
            # Critical Network/API Failure - The Worker will handle the Rollback
            logger.opt(exception=True).error(f"LLM Batch Critical Failure: {e}")
            raise e
