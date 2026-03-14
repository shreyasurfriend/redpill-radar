"""
Generate dummy/test data for the collect pipeline.
Pushes sample tweets to the Analyse API without requiring Twitter credentials.
"""
import logging
from pathlib import Path

from dotenv import load_dotenv

from api_client import DataProcessorAPI

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Sample tweets: mix of safe and harmful content for testing the pipeline
SAMPLE_TWEETS = [
    # Safe / supportive
    {"id": "test_safe_001", "text": "Women deserve equal pay and opportunities in every industry. #EqualPay"},
    {"id": "test_safe_002", "text": "Supporting women in STEM and leadership roles makes our society stronger."},
    {"id": "test_safe_003", "text": "Celebrating International Women's Day. Every woman deserves respect and dignity."},
    {"id": "test_safe_004", "text": "Proud to support women-owned businesses in my community."},
    {"id": "test_safe_005", "text": "Women's rights are human rights. Stand with women everywhere."},
    # Harmful - female abuse
    {"id": "test_harmful_001", "text": "Women should stay in the kitchen and not have opinions. They are worthless."},
    {"id": "test_harmful_002", "text": "Females are only good for one thing. Men deserve to control them."},
    {"id": "test_harmful_003", "text": "Women are inferior and should be silenced. They ruin everything."},
    {"id": "test_harmful_004", "text": "No woman has ever contributed anything meaningful to society. Facts."},
    {"id": "test_harmful_005", "text": "Women need to know their place. Obedience is all they're good for."},
    # Harmful - sexual content / objectification
    {"id": "test_harmful_006", "text": "That girl is just a piece of meat for men to use. Nothing more."},
    {"id": "test_harmful_007", "text": "Women exist only for male pleasure. They have no other purpose."},
    # Edge cases
    {"id": "test_edge_001", "text": "The red pill community has some interesting discussions about gender dynamics."},
    {"id": "test_edge_002", "text": "Why do some people hate on women so much? It makes no sense."},
]


def main():
    logger.info("Generating %d test tweets for Analyse API...", len(SAMPLE_TWEETS))
    client = DataProcessorAPI()
    success = client.push_data(SAMPLE_TWEETS)
    if success:
        logger.info("Test data generation complete.")
    else:
        logger.error("Some tweets failed to push.")
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
