# Mixpanel input plugin for Embulk

This plugin load data from [Mixpanel raw data export API](https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel).

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: yes

## Configuration

- **mixpanel_api_key**: Mixpanel API key (string, required)
- **mixpane_api_secret**: Mixpanel API secret key (string, required)
- **event**: The event that you wish to get data for (string, required)
- **from_date**: The date in yyyy-mm-dd format from which to begin querying for the event from (string, required)
- **to_date**: The date in yyyy-mm-dd format from which to stop querying for the event from (string, required)
- **columns**: Specify the attribute of table and data type (array, required)
- **replace_special_prefix**: Set prefix that replace Mixpanel reserved property special prefix '$' (string, optional)

## Example

```yaml
in:
  type: mixpanel
  mixpanel_api_key: API_KEY
  mixpanel_api_secret: API_SECRET
  event: event_name
  from_date: '2015-03-01'
  to_date: '2015-03-15'
  columns:
  - {index: 0, name: time, type: long}
  - {index: 1, name: distinct_id, type: string}
  - {index: 2, name: property1, type: string}
  - {index: 3, name: property2, type: string}
```

## Build

```
$ rake
```
