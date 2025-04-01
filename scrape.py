import asyncio
import json
import csv
import traceback
import re # Import regex for potential splitting later
from bs4 import BeautifulSoup, NavigableString
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, JsonCssExtractionStrategy

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
            # --- CHANGE THIS LINE ---
            "type": "html"
            # --- END CHANGE ---
        },
        {
            "name": "value_manhwa",
            "selector": "td.pi-data-value[data-source='manhwa']",
            "type": "text" # Keep text for specific debut fields
        },
        {
            "name": "value_webnovel",
            "selector": "td.pi-data-value[data-source='webnovel']",
            "type": "text" # Keep text for specific debut fields
        },
    ]
}

import asyncio
import json
import csv
import traceback
import re # Import regex for potential splitting later
from bs4 import BeautifulSoup # Import BeautifulSoup
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, JsonCssExtractionStrategy

# --- Schema definition as modified in Step 1 ---
infobox_schema_explicit = {
    "name": "CharacterInfoExplicit",
    "baseSelector": "aside.portable-infobox section.pi-group, aside.portable-infobox div.pi-item, aside.portable-infobox tr",
    "fields": [
        {"name": "section_header", "selector": "h2.pi-title, :scope:is(h2.pi-item)", "type": "text"},
        {"name": "label", "selector": "h3.pi-data-label, th.pi-data-label, td.pi-data-label", "type": "text"},
        {"name": "value", "selector": "div.pi-data-value, td.pi-data-value:not([data-source])", "type": "html"}, # Changed to html
        {"name": "value_manhwa", "selector": "td.pi-data-value[data-source='manhwa']", "type": "text"},
        {"name": "value_webnovel", "selector": "td.pi-data-value[data-source='webnovel']", "type": "text"},
    ]
}

# --- Define keys that might contain multiple separable items ---
MULTI_ITEM_KEYS = {
    "Alias(es)",
    "Occupation(s)",
    "Affiliation(s)",
    "Relatives",
    "Vital Status",
    "Age",
    # Add any other keys you observe having this issue
}

async def get_character_info(character_url, crawler):
    """
    Crawls character page, extracts fields, parses HTML for multi-item fields,
    and handles Debut fields. Attempts multiple strategies for splitting items.
    """
    print(f"Fetching info for: {character_url}")
    config = CrawlerRunConfig(
        extraction_strategy=JsonCssExtractionStrategy(schema=infobox_schema_explicit),
    )

    result = await crawler.arun(url=character_url, config=config)

    if not result.success:
        print(f"Crawl failed for {character_url}: {result.error_message}")
        return None

    if result.extracted_content:
        try:
            extracted_list = json.loads(result.extracted_content)
            character_data = {}

            for item_dict in extracted_list: # Renamed item to item_dict to avoid confusion
                label = item_dict.get("label")
                value_html = item_dict.get("value")

                if label and value_html and label.strip() and value_html.strip():
                    label_clean = label.strip()

                    # Skip Debut labels here, handled later
                    if label_clean.startswith("Debut"):
                        continue

                    processed_value = None
                    items = [] # Initialize items list for this field

                    # Process fields potentially containing multiple items
                    if label_clean in MULTI_ITEM_KEYS and '<' in value_html and '>' in value_html:
                        soup = BeautifulSoup(value_html, 'html.parser')
                        current_item_parts = []

                        # Iterate through all descendant nodes, not just direct children
                        for element in soup.descendants:
                            if isinstance(element, NavigableString):
                                # Add stripped text if it's not just whitespace
                                text = element.strip()
                                if text:
                                    current_item_parts.append(text)
                            elif element.name == 'br':
                                # If we encounter a <br>, join the parts collected so far for the *previous* item
                                if current_item_parts:
                                    items.append(" ".join(current_item_parts))
                                    current_item_parts = [] # Reset for the next item
                            # Add other block-level tags that might imply separation if needed
                            # elif element.name in ['div', 'p', 'li']:
                            #     if current_item_parts:
                            #         items.append(" ".join(current_item_parts))
                            #         current_item_parts = []

                        # Add any remaining parts after the loop finishes (for the last item)
                        if current_item_parts:
                            items.append(" ".join(current_item_parts))

                        # --- Cleanup and final decision ---
                        # Filter out any potential empty strings resulting from multiple separators
                        items = [re.sub(r'\s+', ' ', item).strip() for item in items if item.strip()]

                        # If iteration didn't yield multiple items, fall back to text of common elements
                        if len(items) <= 1:
                            # Try getting text specifically from links or list items if they exist
                            links = soup.find_all(['a', 'li']) # Check for links or list items
                            if len(links) > 1:
                                candidate_items = [el.get_text(strip=True) for el in links if el.get_text(strip=True)]
                                if len(candidate_items) > 1:
                                    items = candidate_items

                        # If still only one item (or none), assign the cleaned full text
                        if len(items) <= 1 :
                             full_text = soup.get_text(separator=' ', strip=True)
                             # Heuristic: Try splitting based on CamelCase as a last resort if no spaces exist
                             if items and ' ' not in items[0] and re.search(r'[a-z][A-Z]', items[0]):
                                 potential_split = re.sub(r"(\w)([A-Z])", r"\1 \2", items[0]).split(' ')
                                 if len(potential_split) > 1:
                                     items = [p.strip() for p in potential_split]
                                     print(f"Used CamelCase split for: {label_clean}")
                                 else: # CamelCase didn't work, use full text
                                    items = [full_text]

                             elif not items: # No items found at all previously
                                items = [full_text] if full_text else []


                        # Final assignment
                        processed_value = items if items else soup.get_text(separator=' ', strip=True)

                        # Ensure single-item lists aren't trivial artifacts
                        if isinstance(processed_value, list) and len(processed_value) == 1:
                             # If the only item is identical to the full text, just use the string
                             full_text_check = soup.get_text(separator=' ', strip=True)
                             if processed_value[0] == full_text_check:
                                   processed_value = full_text_check
                             # Optional: Add comma splitting heuristic here if needed


                    else: # Not a multi-item key or not HTML
                        if isinstance(value_html, str) and '<' in value_html and '>' in value_html:
                             soup = BeautifulSoup(value_html, 'html.parser')
                             processed_value = soup.get_text(separator=' ', strip=True)
                        else:
                             processed_value = str(value_html).strip()

                    character_data[label_clean] = processed_value

                # --- Debut Logic (remains the same) ---
                for source in ["manhwa", "webnovel"]:
                    source_value = item_dict.get(f"value_{source}")
                    if source_value and source_value.strip():
                        character_data[f"Debut ({source.capitalize()})"] = source_value.strip()

            print("--- Extracted Data ---")
            print(json.dumps(character_data, indent=2))
            print("----------------------")
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
        print(f"No structured content extracted for {character_url}.")
        return None


async def process_csv(filename, crawler):
    character_dict = {}
    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        tasks = []

        for row in reader:
            if len(row) != 2:
                print(f"Skipping invalid row: {row}")
                continue
            character_name, character_url = row
            print(f"Processing: {character_name} -> {character_url}")
            tasks.append(get_character_info(character_url, crawler))
            character_dict[character_name] = None

        if not tasks:
            print("No tasks to process!")
            return {}

        # Execute all async tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Populate dictionary with results
        for (character_name, result) in zip(character_dict.keys(), results):
            if isinstance(result, Exception):
                print(f"Error processing {character_name}: {result}")
            character_dict[character_name] = result

    return character_dict

async def main():
    filename = "character_pages.csv"
    async with AsyncWebCrawler() as crawler:
        await process_csv(filename, crawler)
        #print(character_info_dict)

if __name__ == "__main__":
    asyncio.run(main())
