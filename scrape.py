import asyncio
import json
from pydantic import BaseModel
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, JsonCssExtractionStrategy
import csv
import traceback

infobox_schema_explicit = {
    "name": "CharacterInfoExplicit",
    "baseSelector": "aside.portable-infobox section.pi-group, aside.portable-infobox div.pi-item, aside.portable-infobox tr",
    "fields": [
        {
          "name": "section_header",
          "selector": "h2.pi-title, :scope:is(h2.pi-item)",
          "type": "text"
        },
        {
          "name": "label",
          "selector": "h3.pi-data-label, th.pi-data-label, td.pi-data-label",
          "type": "text"
        },
        {
          "name": "value",
          "selector": "div.pi-data-value, td.pi-data-value:not([data-source])",
          "type": "text"
        },
        {
          "name": "value_manhwa",
          "selector": "td.pi-data-value[data-source='manhwa']",
          "type": "text"
        },
        {
          "name": "value_webnovel",
          "selector": "td.pi-data-value[data-source='webnovel']",
          "type": "text"
        },
    ]
}


async def getCharacters():
    config = CrawlerRunConfig(
        css_selector="#mw-content-text > div.category-page__members",
    )
    
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(
            url="https://return-of-the-blossoming-blade.fandom.com/wiki/Category:Characters", 
            config=config
        )
        if not result.success:
            print("Crawl failed:", result.error_message)
            return

        print(result.markdown)


async def get_character_info(character_url):
    """
    Crawls character page, extracts fields including specific data-sources,
    and assumes 'value_manhwa'/'value_webnovel' belong to 'Debut'.
    """
    print(f"Fetching info for: {character_url}")
    config = CrawlerRunConfig(
        extraction_strategy=JsonCssExtractionStrategy(schema=infobox_schema_explicit),
    )

    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=character_url, config=config)

        if not result.success:
            print(f"Crawl failed for {character_url}: {result.error_message}")
            return None

        if result.extracted_content:
            try:
                extracted_list = json.loads(result.extracted_content)
                print(f"\nDEBUG Raw extracted list for {character_url}:\n{json.dumps(extracted_list, indent=2)}") # Keep debug for now

                character_data = {}
                current_section = None

                for i, item in enumerate(extracted_list):
                    header = item.get("section_header")
                    if header and header.strip():
                        current_section = header.strip()
                        print(f"DEBUG: Found section header '{current_section}' at index {i}")
                        continue

                    # Check for simple label/value pairs
                    label = item.get("label")
                    value = item.get("value")
                    if label and value and label.strip() and value.strip():
                        # Prevent overwriting already found Debut values if duplicates exist
                        if not label.strip().startswith("Debut"):
                           character_data[label.strip()] = value.strip()
                           print(f"DEBUG: Found simple pair: '{label.strip()}': '{value.strip()}' at index {i}")

                    # --- Hardcoded Logic for Debut Values ---
                    for source in ["manhwa", "webnovel"]:
                        source_key = f"value_{source}"
                        source_value = item.get(source_key)

                        if source_value and source_value.strip():
                            section_name = "Debut"
                            compound_key = f"{section_name} ({source.capitalize()})"

                            # Add to dictionary, potentially overwriting if found multiple times
                            character_data[compound_key] = source_value.strip()
                            print(f"DEBUG: Hardcoded '{source_value.strip()}' as '{compound_key}' from item index {i}")

                # Remove duplicates/empty values (simple cleanup)
                # Using a second pass to prioritize non-null over null if keys clash, but the hardcoding logic should handle it mostly
                final_data = {}
                temp_section = None
                for item in extracted_list: 
                     header = item.get("section_header")
                     if header and header.strip(): temp_section = header.strip()

                     label = item.get("label")
                     value = item.get("value")
                     if label and value and label.strip() and value.strip():
                          if not label.strip().startswith("Debut") or label.strip() not in final_data:
                             final_data[label.strip()] = value.strip()

                     for source in ["manhwa", "webnovel"]:
                         source_key = f"value_{source}"
                         source_value = item.get(source_key)
                         if source_value and source_value.strip():
                             compound_key = f"Debut ({source.capitalize()})"
                             final_data[compound_key] = source_value.strip() # Ensure latest found value is kept

                character_data = final_data # Assign cleaned data


                if not character_data:
                    print(f"Warning: Final character_data dictionary is empty for {character_url}.")
                print(character_data)
                return character_data

            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON for {character_url}: {e}")
                print(f"Raw extracted content: {result.extracted_content}")
                return None
            except Exception as e:
                print(f"Error processing extracted data for {character_url}: {e}")
                traceback.print_exc()
                return None
        else:
            print(f"No structured content extracted (result.extracted_content is empty) for {character_url}.")
            return None

async def main():
    #await getCharacters()
    await get_character_info("https://return-of-the-blossoming-blade.fandom.com/wiki/Cheongmun")

if __name__ == "__main__":
    asyncio.run(main())