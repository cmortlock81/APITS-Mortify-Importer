<?php
/**
 * Plugin Name: APITS Mortify Importer
 * Description: Imports APITS agent listings into WordPress posts with images and keeps them in sync.
 * Version: 1.0.0
 * Author: APITS
 * Text Domain: apits-mortify-importer
 */

if (! defined('ABSPATH')) {
    exit;
}

define('APITS_MORTIFY_IMPORTER_VERSION', '1.0.0');
define('APITS_MORTIFY_IMPORTER_FILE', __FILE__);
define('APITS_MORTIFY_IMPORTER_PATH', plugin_dir_path(__FILE__));
define('APITS_MORTIFY_IMPORTER_URL', plugin_dir_url(__FILE__));

require_once APITS_MORTIFY_IMPORTER_PATH . 'includes/class-apits-mortify-importer.php';

APITS_Mortify_Importer::instance();
