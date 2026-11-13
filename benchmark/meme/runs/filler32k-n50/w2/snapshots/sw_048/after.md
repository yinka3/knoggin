# Project Context

## Current State
C1 [agent_derived; support: M1]
The user is creating Solopreneur AI, a SaaS app to help users find a niche, create offers, develop products with AI, write sales copy, and generate content plans.

C2 [user_asserted; support: M2]
The team's default branch is main. They are setting up CI/CD pipelines and want to ensure the pipelines track the main branch correctly.

C3 [user_asserted; support: M3]
The team uses Veltrion Pkg as their package manager, having switched from Dranith Stack.

C4 [user_asserted; support: M4]
The team uses Zenthos ID for authentication, replacing the previous Cerberix-SAML-assertion token format.

C5 [user_asserted; support: M5]
The team meeting is every Friday at 9:30am for syncing project updates and discussing blockers.

C6 [user_asserted; support: M6, M7, M8]
The user is working with Microsoft Graph API to filter group resources by displayName and to list group members' mail properties. They have successfully retrieved group members' mail values and are now parsing this data using Python and Pandas to extract email addresses into a DataFrame.

C7 [user_asserted; support: M9, M10, M11]
The user is exploring goal management systems that expose APIs for integration with other products. They have requested lists and tables of such systems, including large and expensive options, sorted by pricing.


## Active Work
C8 [agent_derived; support: M12, M13]
The user requested an ASP.NET purchase forecasting app example considering stock, historical orders, and sales peaks. The assistant provided a conceptual outline and sample code with a GridView showing stock and a button for automatic ordering, currently showing a confirmation message.

C9 [agent_derived; support: M14]
The user requested a 10-minute video script for the Winix 5500-2 Air Cleaner highlighting Plasma Wave Technology, filtration system, ease of use, and air quality improvements, while fairly presenting cons like filter replacement frequency and noise. The script is first-person, engaging, and aligned with Amazon's style.

C10 [agent_derived; support: M15]
The user is working on a Python script that downloads financial data for multiple ticker symbols from Yahoo Finance and FRED using pandas_datareader and yfinance. The script overrides pandas_datareader's default method to use yfinance, sets a date range from 2008-01-01 to 2023-03-01, and defines ticker symbols with their data sources. It downloads weekly adjusted close prices or relevant data, resamples to weekly averages, drops nulls, renames columns to human-readable names, concatenates all data into a master DataFrame, and fills any remaining null values with column means.

C11 [user_asserted; support: M16, M17, M18]
The user requested Arduino code to communicate with the Sensirion SHT4x sensor via I2C, sending temperature and humidity data as binary ASCII optimized for LabVIEW. The assistant provided a complete Arduino sketch that initializes I2C, commands the sensor, reads 6 bytes of data, converts raw sensor data to temperature and humidity floats, packs them into an 8-byte buffer, and sends this buffer over serial.

Additionally, the assistant described how to connect the sensor to Arduino pins (VCC to 3.3V, GND to GND, SDA to A4, SCL to A5), set up the Arduino IDE, install the Sensirion library, compile, and flash the code.

Finally, the assistant explained how to receive this binary ASCII data in LabVIEW: configuring the serial port with VISA, converting the incoming string to a byte array, indexing bytes, unflattening from byte stream to extract temperature and humidity floats, displaying these values on numeric indicators, and plotting them on a waveform graph in real-time.

This covers the full data acquisition and visualization pipeline from sensor to LabVIEW graph.

C12 [user_asserted; support: M19]
The user requested code on how to reset a password in Laravel as an API endpoint. The assistant provided a complete example including:

- A POST route `/reset-password` in `routes/api.php` pointing to `ResetPasswordController@reset`.
- A `ResetPasswordController` with a `reset` method validating the token, email, password, and password confirmation, then using Laravel's `Password::reset` method to update the user's password with bcrypt hashing.
- Instructions to register the controller routes in `RouteServiceProvider`.
- Example JSON payload for the API call with `token`, `email`, `password`, and `password_confirmation` fields.
- Expected JSON responses for success (200) and failure (422).

This provides a full Laravel API password reset implementation example.

C13 [user_asserted; support: M20]
The user requested a SharePoint Framework application customizer that injects the Google Tag Manager snippet into the page header. The assistant provided a step-by-step example including:

- Creating a new SPFx solution with `yo @microsoft/sharepoint` and selecting "Application customizer".
- Editing the generated `MyExtensionApplicationCustomizer.ts` to add script and noscript tags for Google Tag Manager with placeholders for the GTM container ID.
- Instructions to build, bundle, package, and deploy the solution to the SharePoint site.

This enables automatic injection of the GTM snippet on all pages of the site.


## Decisions and Constraints
C14 [user_asserted; support: M21, M22, M23]
The user is implementing a SwiftUI MVVM navigation stack for iPadOS 16 with groups and items loaded from a model. They have moved from static data to loading groups from an API, initially mocked, then using URLSession for live calls. They want to conditionally switch between mock data and live API calls for testing purposes.

C15 [user_asserted; support: M24]
The user wants to build Solopreneur AI using APIs and AI to complete each section of the app. The app is broken down into five sections: 1) Niche Research, 2) Offer Creation, 3) Product Creation, 4) Sales copy / Copywriting, and 5) Content Creation and Scheduling.

C16 [user_asserted; support: M25]
The team uses a paired on-call rotation system with primary and secondary roles to ensure support coverage.

C17 [user_asserted; support: M26, M27, M28]
The user currently uses Zenthos ID for authentication with SAML 2.0 SSO and the auth middleware oathkeeper-mtls-proxy v4.1. If they switch auth providers, the login endpoint and authentication method will change accordingly, for example to OAuth 2.0 + PKCE with a new login endpoint /auth/kryptal/signin and middleware verithos-oauth-middleware v2.1. The user is aware these changes require updating configurations, testing integration, and ensuring compatibility with token validation and session management.


## Preferences

## Open Questions
C18 [user_asserted; support: M29]
The user asked for a Windows CMD script that runs the "dir" command and appends the output to a text file, suitable for use in a scheduled task. The assistant provided a step-by-step explanation on creating a batch script with `dir >> output.txt` to append output, instructions for saving it as a `.cmd` file, and detailed steps to configure a scheduled task to run this script, ensuring the output file appends new data each time. The assistant also cited relevant web sources for batch scripting and task scheduling.

C19 [agent_derived; support: M30]
The user reported an exact error log for a deadlock situation: "DeadlockDetected: transactions 4821 and 4823 are deadlocked on table aurora_orders. Query 1 was UPDATE orders SET status = 'shipped' WHERE region = 'us-east'. Query 2 was DELETE FROM orders WHERE created_at < NOW() - INTERVAL '30 days'. First seen at 2024-08-04T22:33:18Z; recommend serializable isolation review." They seek tips or things to watch out for regarding this error.

C20 [user_asserted; support: M31]
The user asked whether, when selecting a field in Python with Selenium using a CSS selector like `textarea.lmt__textarea.lmt__source_textarea.lmt__textarea_base_style`, it is necessary to specify all classes of the field or if specifying only one class that occurs there is sufficient. The assistant explained that specifying any one of the classes present in the element's class list is sufficient to select the element, but in practice, combining classes or other attributes is recommended to uniquely identify the element and avoid confusion with other elements on the page.

Support handles:
- M1: user_message
- M2: user_message
- M3: user_message
- M4: user_message
- M5: user_message
- M6: user_message
- M7: user_message
- M8: user_message
- M9: user_message
- M10: user_message
- M11: user_message
- M12: user_message
- M13: user_message
- M14: user_message
- M15: user_message
- M16: user_message
- M17: user_message
- M18: user_message
- M19: user_message
- M20: user_message
- M21: user_message
- M22: user_message
- M23: user_message
- M24: user_message
- M25: user_message
- M26: user_message
- M27: user_message
- M28: user_message
- M29: user_message
- M30: user_message
- M31: user_message
