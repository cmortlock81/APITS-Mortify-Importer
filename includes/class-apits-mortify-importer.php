<?php

if (! defined('ABSPATH')) {
    exit;
}

class APITS_Mortify_Importer
{
    private static $instance;

    const CPT = 'apits_property';
    const RUNS_TABLE = 'apits_mortify_import_runs';
    const JOBS_TABLE = 'apits_mortify_jobs';

    const JOB_DISCOVER = 'discover';
    const JOB_IMPORT_LISTING = 'import_listing';
    const JOB_FINALIZE = 'finalize';

    public static function instance()
    {
        if (! self::$instance) {
            self::$instance = new self();
        }

        return self::$instance;
    }

    private function __construct()
    {
        register_activation_hook(APITS_MORTIFY_IMPORTER_FILE, [$this, 'activate']);
        register_deactivation_hook(APITS_MORTIFY_IMPORTER_FILE, [$this, 'deactivate']);

        add_action('init', [$this, 'register_content_model']);
        add_action('admin_menu', [$this, 'register_admin_menu']);
        add_action('admin_init', [$this, 'register_settings']);

        add_action('apits_mortify_importer_process_queue', [$this, 'process_queue']);
        add_action('apits_mortify_importer_daily_sync', [$this, 'start_scheduled_sync']);

        add_action('admin_post_apits_mortify_importer_run_now', [$this, 'handle_manual_run']);

        add_filter('cron_schedules', [$this, 'register_cron_schedule']);

        if (defined('WP_CLI') && WP_CLI) {
            $this->register_cli();
        }
    }

    public function activate()
    {
        $this->create_tables();
        $this->register_content_model();
        flush_rewrite_rules();

        if (! wp_next_scheduled('apits_mortify_importer_process_queue')) {
            wp_schedule_event(time() + 60, 'every_minute', 'apits_mortify_importer_process_queue');
        }

        if (! wp_next_scheduled('apits_mortify_importer_daily_sync')) {
            wp_schedule_event(time() + HOUR_IN_SECONDS, 'daily', 'apits_mortify_importer_daily_sync');
        }
    }

    public function deactivate()
    {
        wp_clear_scheduled_hook('apits_mortify_importer_process_queue');
        wp_clear_scheduled_hook('apits_mortify_importer_daily_sync');
        flush_rewrite_rules();
    }

    public function register_cron_schedule($schedules)
    {
        $schedules['every_minute'] = [
            'interval' => 60,
            'display'  => __('Every Minute', 'apits-mortify-importer'),
        ];

        return $schedules;
    }

    public function register_content_model()
    {
        register_post_type(self::CPT, [
            'label' => __('APITS Properties', 'apits-mortify-importer'),
            'public' => true,
            'show_in_menu' => true,
            'menu_icon' => 'dashicons-building',
            'supports' => ['title', 'editor', 'excerpt', 'thumbnail'],
            'has_archive' => true,
            'show_in_rest' => true,
        ]);

        register_taxonomy('apits_country', self::CPT, [
            'label' => __('Country', 'apits-mortify-importer'),
            'public' => true,
            'hierarchical' => true,
            'show_in_rest' => true,
        ]);
        register_taxonomy('apits_region', self::CPT, [
            'label' => __('Region', 'apits-mortify-importer'),
            'public' => true,
            'hierarchical' => true,
            'show_in_rest' => true,
        ]);
        register_taxonomy('apits_city', self::CPT, [
            'label' => __('City', 'apits-mortify-importer'),
            'public' => true,
            'hierarchical' => true,
            'show_in_rest' => true,
        ]);
        register_taxonomy('apits_property_type', self::CPT, [
            'label' => __('Property Type', 'apits-mortify-importer'),
            'public' => true,
            'hierarchical' => true,
            'show_in_rest' => true,
        ]);

        register_post_status('archived', [
            'label' => _x('Archived', 'post', 'apits-mortify-importer'),
            'public' => false,
            'internal' => false,
            'protected' => true,
            'exclude_from_search' => true,
            'show_in_admin_all_list' => true,
            'show_in_admin_status_list' => true,
            'label_count' => _n_noop('Archived <span class="count">(%s)</span>', 'Archived <span class="count">(%s)</span>', 'apits-mortify-importer'),
        ]);
    }

    public function register_settings()
    {
        register_setting('apits_mortify_importer', 'apits_mortify_importer_settings', [$this, 'sanitize_settings']);

        add_settings_section('apits_mortify_importer_main', __('Importer Settings', 'apits-mortify-importer'), '__return_false', 'apits-mortify-importer');

        $fields = [
            'portfolio_url' => __('Portfolio URL', 'apits-mortify-importer'),
            'post_status' => __('Post Status on Create', 'apits-mortify-importer'),
            'update_behavior' => __('Update Behavior', 'apits-mortify-importer'),
            'archive_behavior' => __('Archive Behavior', 'apits-mortify-importer'),
            'throttle_seconds' => __('Throttle Seconds', 'apits-mortify-importer'),
            'acf_map' => __('Write compatible ACF aliases', 'apits-mortify-importer'),
        ];

        foreach ($fields as $key => $label) {
            add_settings_field(
                'apits_mortify_importer_' . $key,
                $label,
                [$this, 'render_settings_field'],
                'apits-mortify-importer',
                'apits_mortify_importer_main',
                ['key' => $key]
            );
        }
    }

    public function sanitize_settings($input)
    {
        $defaults = $this->get_settings();
        $output = [];

        $output['portfolio_url'] = isset($input['portfolio_url']) ? esc_url_raw(trim($input['portfolio_url'])) : $defaults['portfolio_url'];
        $output['post_status'] = in_array($input['post_status'] ?? '', ['draft', 'publish'], true) ? $input['post_status'] : 'draft';
        $output['update_behavior'] = in_array($input['update_behavior'] ?? '', ['full', 'meta_only'], true) ? $input['update_behavior'] : 'full';
        $output['archive_behavior'] = in_array($input['archive_behavior'] ?? '', ['draft', 'archived'], true) ? $input['archive_behavior'] : 'draft';
        $output['throttle_seconds'] = max(1, min(30, absint($input['throttle_seconds'] ?? 3)));
        $output['acf_map'] = empty($input['acf_map']) ? 0 : 1;

        return $output;
    }

    public function get_settings()
    {
        $defaults = [
            'portfolio_url' => 'https://www.aplaceinthesun.com/property/agent/521573/dcts-real-estate-development-llc',
            'post_status' => 'draft',
            'update_behavior' => 'full',
            'archive_behavior' => 'draft',
            'throttle_seconds' => 3,
            'acf_map' => 1,
        ];

        return wp_parse_args(get_option('apits_mortify_importer_settings', []), $defaults);
    }

    public function render_settings_field($args)
    {
        $settings = $this->get_settings();
        $key = $args['key'];

        if ($key === 'portfolio_url') {
            printf('<input type="url" name="apits_mortify_importer_settings[%1$s]" value="%2$s" class="regular-text" />', esc_attr($key), esc_attr($settings[$key]));
            return;
        }

        if ($key === 'throttle_seconds') {
            printf('<input type="number" min="1" max="30" name="apits_mortify_importer_settings[%1$s]" value="%2$d" />', esc_attr($key), (int) $settings[$key]);
            return;
        }

        if ($key === 'acf_map') {
            printf('<label><input type="checkbox" name="apits_mortify_importer_settings[%1$s]" value="1" %2$s /> %3$s</label>', esc_attr($key), checked(1, (int) $settings[$key], false), esc_html__('Also write alias meta keys for ACF field mapping.', 'apits-mortify-importer'));
            return;
        }

        $options = [];
        if ($key === 'post_status') {
            $options = ['draft' => 'Draft', 'publish' => 'Publish'];
        } elseif ($key === 'update_behavior') {
            $options = ['full' => 'Overwrite title/content/meta', 'meta_only' => 'Update meta only'];
        } elseif ($key === 'archive_behavior') {
            $options = ['draft' => 'Set to Draft', 'archived' => 'Set to Archived'];
        }

        echo '<select name="apits_mortify_importer_settings[' . esc_attr($key) . ']">';
        foreach ($options as $value => $label) {
            echo '<option value="' . esc_attr($value) . '" ' . selected($settings[$key], $value, false) . '>' . esc_html($label) . '</option>';
        }
        echo '</select>';
    }

    public function register_admin_menu()
    {
        add_management_page(
            __('APITS Mortify Importer', 'apits-mortify-importer'),
            __('APITS Mortify Importer', 'apits-mortify-importer'),
            'manage_options',
            'apits-mortify-importer',
            [$this, 'render_admin_page']
        );
    }

    public function render_admin_page()
    {
        if (! current_user_can('manage_options')) {
            wp_die(esc_html__('You do not have permission.', 'apits-mortify-importer'));
        }

        global $wpdb;
        $runs_table = $wpdb->prefix . self::RUNS_TABLE;
        $jobs_table = $wpdb->prefix . self::JOBS_TABLE;

        $last_run = $wpdb->get_row("SELECT * FROM {$runs_table} ORDER BY id DESC LIMIT 1", ARRAY_A);
        $failed_jobs = $wpdb->get_results($wpdb->prepare("SELECT * FROM {$jobs_table} WHERE run_id = %d AND status = 'failed' ORDER BY id DESC LIMIT 20", $last_run['id'] ?? 0), ARRAY_A);
        ?>
        <div class="wrap">
            <h1><?php esc_html_e('APITS Mortify Importer', 'apits-mortify-importer'); ?></h1>

            <form method="post" action="options.php">
                <?php
                settings_fields('apits_mortify_importer');
                do_settings_sections('apits-mortify-importer');
                submit_button(__('Save Settings', 'apits-mortify-importer'));
                ?>
            </form>

            <hr />
            <h2><?php esc_html_e('Actions', 'apits-mortify-importer'); ?></h2>
            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
                <?php wp_nonce_field('apits_mortify_importer_run_now'); ?>
                <input type="hidden" name="action" value="apits_mortify_importer_run_now" />
                <?php submit_button(__('Run import now', 'apits-mortify-importer'), 'primary', 'submit', false); ?>
            </form>

            <hr />
            <h2><?php esc_html_e('Last run summary', 'apits-mortify-importer'); ?></h2>
            <?php if ($last_run) : ?>
                <table class="widefat striped">
                    <tbody>
                    <?php foreach ($last_run as $key => $value) : ?>
                        <tr>
                            <th><?php echo esc_html($key); ?></th>
                            <td><?php echo esc_html((string) $value); ?></td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            <?php else : ?>
                <p><?php esc_html_e('No runs yet.', 'apits-mortify-importer'); ?></p>
            <?php endif; ?>

            <h2><?php esc_html_e('Recent errors', 'apits-mortify-importer'); ?></h2>
            <?php if (! empty($failed_jobs)) : ?>
                <table class="widefat striped">
                    <thead>
                        <tr><th>ID</th><th>Type</th><th>Payload</th><th>Error</th></tr>
                    </thead>
                    <tbody>
                    <?php foreach ($failed_jobs as $job) : ?>
                        <tr>
                            <td><?php echo (int) $job['id']; ?></td>
                            <td><?php echo esc_html($job['job_type']); ?></td>
                            <td><code><?php echo esc_html($job['payload_json']); ?></code></td>
                            <td><?php echo esc_html($job['last_error']); ?></td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            <?php else : ?>
                <p><?php esc_html_e('No errors in the last run.', 'apits-mortify-importer'); ?></p>
            <?php endif; ?>
        </div>
        <?php
    }

    public function handle_manual_run()
    {
        if (! current_user_can('manage_options')) {
            wp_die(esc_html__('Permission denied.', 'apits-mortify-importer'));
        }

        check_admin_referer('apits_mortify_importer_run_now');

        $this->start_run('manual');

        wp_safe_redirect(admin_url('tools.php?page=apits-mortify-importer&apits_run=1'));
        exit;
    }

    public function start_scheduled_sync()
    {
        $this->start_run('cron');
    }

    private function start_run($trigger)
    {
        global $wpdb;
        $runs_table = $wpdb->prefix . self::RUNS_TABLE;

        $wpdb->insert($runs_table, [
            'started_at' => current_time('mysql'),
            'trigger_type' => $trigger,
            'status' => 'running',
            'total_discovered' => 0,
            'total_processed' => 0,
            'total_created' => 0,
            'total_updated' => 0,
            'total_failed' => 0,
            'total_archived' => 0,
            'last_error' => '',
        ]);

        $run_id = (int) $wpdb->insert_id;
        $this->enqueue_job($run_id, self::JOB_DISCOVER, [
            'portfolio_url' => $this->get_settings()['portfolio_url'],
        ]);
    }

    private function enqueue_job($run_id, $job_type, $payload, $delay_seconds = 0)
    {
        global $wpdb;
        $jobs_table = $wpdb->prefix . self::JOBS_TABLE;
        $now = time();
        $locked_until = $delay_seconds > 0 ? gmdate('Y-m-d H:i:s', $now + $delay_seconds) : gmdate('Y-m-d H:i:s', $now - 1);

        $wpdb->insert($jobs_table, [
            'run_id' => (int) $run_id,
            'job_type' => sanitize_key($job_type),
            'payload_json' => wp_json_encode($payload),
            'status' => 'queued',
            'attempts' => 0,
            'last_error' => '',
            'locked_until' => $locked_until,
            'created_at' => current_time('mysql'),
            'updated_at' => current_time('mysql'),
        ]);
    }

    public function process_queue()
    {
        global $wpdb;
        $jobs_table = $wpdb->prefix . self::JOBS_TABLE;
        $now = gmdate('Y-m-d H:i:s');

        $job = $wpdb->get_row($wpdb->prepare(
            "SELECT * FROM {$jobs_table} WHERE status IN ('queued','retrying') AND locked_until <= %s ORDER BY id ASC LIMIT 1",
            $now
        ), ARRAY_A);

        if (! $job) {
            return;
        }

        $wpdb->update($jobs_table, [
            'status' => 'running',
            'updated_at' => current_time('mysql'),
        ], ['id' => (int) $job['id']]);

        $payload = json_decode($job['payload_json'], true);

        try {
            if ($job['job_type'] === self::JOB_DISCOVER) {
                $this->run_discover_job((int) $job['run_id'], $payload);
            } elseif ($job['job_type'] === self::JOB_IMPORT_LISTING) {
                $this->run_listing_job((int) $job['run_id'], $payload);
            } elseif ($job['job_type'] === self::JOB_FINALIZE) {
                $this->run_finalize_job((int) $job['run_id']);
            }

            $wpdb->update($jobs_table, [
                'status' => 'success',
                'updated_at' => current_time('mysql'),
            ], ['id' => (int) $job['id']]);
        } catch (Exception $e) {
            $this->fail_or_retry_job($job, $e->getMessage());
        }
    }

    private function fail_or_retry_job($job, $message)
    {
        global $wpdb;
        $jobs_table = $wpdb->prefix . self::JOBS_TABLE;
        $runs_table = $wpdb->prefix . self::RUNS_TABLE;

        $attempts = (int) $job['attempts'] + 1;

        if ($attempts >= 3) {
            $wpdb->update($jobs_table, [
                'status' => 'failed',
                'attempts' => $attempts,
                'last_error' => sanitize_text_field($message),
                'updated_at' => current_time('mysql'),
            ], ['id' => (int) $job['id']]);

            $run = $wpdb->get_row($wpdb->prepare("SELECT total_failed, status FROM {$runs_table} WHERE id = %d", (int) $job['run_id']), ARRAY_A);
            if ($run) {
                $wpdb->update($runs_table, [
                    'total_failed' => ((int) $run['total_failed']) + 1,
                    'status' => $run['status'] === 'running' ? 'partial' : $run['status'],
                    'last_error' => sanitize_text_field($message),
                ], ['id' => (int) $job['run_id']]);
            }

            return;
        }

        $delay = (int) pow(2, $attempts) * 30;
        $wpdb->update($jobs_table, [
            'status' => 'retrying',
            'attempts' => $attempts,
            'last_error' => sanitize_text_field($message),
            'locked_until' => gmdate('Y-m-d H:i:s', time() + $delay),
            'updated_at' => current_time('mysql'),
        ], ['id' => (int) $job['id']]);
    }

    private function run_discover_job($run_id, $payload)
    {
        $portfolio_url = esc_url_raw($payload['portfolio_url'] ?? '');
        if (! $portfolio_url) {
            throw new Exception('Missing portfolio URL');
        }

        $response = wp_remote_get($portfolio_url, [
            'timeout' => 30,
            'user-agent' => 'APITS-Mortify-Importer/' . APITS_MORTIFY_IMPORTER_VERSION,
        ]);

        if (is_wp_error($response)) {
            throw new Exception($response->get_error_message());
        }

        $code = wp_remote_retrieve_response_code($response);
        if ($code < 200 || $code >= 300) {
            throw new Exception('Portfolio request failed with HTTP ' . $code);
        }

        $html = wp_remote_retrieve_body($response);
        $urls = $this->extract_listing_urls($html, $portfolio_url);

        global $wpdb;
        $runs_table = $wpdb->prefix . self::RUNS_TABLE;

        $wpdb->update($runs_table, ['total_discovered' => count($urls)], ['id' => $run_id]);

        $settings = $this->get_settings();
        $throttle = max(1, (int) $settings['throttle_seconds']);

        foreach (array_values($urls) as $index => $url) {
            $delay = $index * $throttle + wp_rand(0, 2);
            $this->enqueue_job($run_id, self::JOB_IMPORT_LISTING, ['url' => $url], $delay);
        }

        $this->enqueue_job($run_id, self::JOB_FINALIZE, [], (count($urls) * $throttle) + 30);
    }

    private function extract_listing_urls($html, $base_url)
    {
        $urls = [];

        libxml_use_internal_errors(true);
        $dom = new DOMDocument();
        $dom->loadHTML($html);
        libxml_clear_errors();

        $xpath = new DOMXPath($dom);
        $nodes = $xpath->query('//a[@href]');

        foreach ($nodes as $node) {
            $href = trim($node->getAttribute('href'));
            if (strpos($href, '/property/details/') === false) {
                continue;
            }

            $urls[] = $this->normalize_url($href, $base_url);
        }

        return array_values(array_unique(array_filter($urls)));
    }

    private function normalize_url($href, $base_url)
    {
        if (strpos($href, 'http') === 0) {
            return esc_url_raw($href);
        }

        $base = wp_parse_url($base_url);
        if (! $base || empty($base['scheme']) || empty($base['host'])) {
            return '';
        }

        return esc_url_raw($base['scheme'] . '://' . $base['host'] . '/' . ltrim($href, '/'));
    }

    private function run_listing_job($run_id, $payload)
    {
        $url = esc_url_raw($payload['url'] ?? '');
        if (! $url) {
            throw new Exception('Missing listing URL');
        }

        $response = wp_remote_get($url, [
            'timeout' => 30,
            'user-agent' => 'APITS-Mortify-Importer/' . APITS_MORTIFY_IMPORTER_VERSION,
        ]);

        if (is_wp_error($response)) {
            throw new Exception($response->get_error_message());
        }

        $code = wp_remote_retrieve_response_code($response);
        if ($code === 429 || $code >= 500) {
            throw new Exception('Transient HTTP ' . $code);
        }
        if ($code < 200 || $code >= 300) {
            throw new Exception('Listing request failed with HTTP ' . $code);
        }

        $html = wp_remote_retrieve_body($response);
        $data = $this->parse_listing($html, $url);
        $hash = hash('sha256', wp_json_encode([$data['title'], $data['description'], $data['meta'], $data['images']]));

        $existing_id = $this->find_existing_post_id($data['meta']['_apits_source_url'], $data['meta']['_apits_listing_ref']);
        $is_new = false;

        if (! $existing_id) {
            $existing_id = wp_insert_post([
                'post_type' => self::CPT,
                'post_status' => $this->get_settings()['post_status'],
                'post_title' => $data['title'],
                'post_content' => $data['description'],
                'post_excerpt' => wp_trim_words(wp_strip_all_tags($data['description']), 40),
            ], true);

            if (is_wp_error($existing_id)) {
                throw new Exception($existing_id->get_error_message());
            }

            $is_new = true;
        }

        $existing_hash = get_post_meta($existing_id, '_apits_hash', true);
        update_post_meta($existing_id, '_apits_last_seen_at', current_time('mysql'));
        update_post_meta($existing_id, '_apits_run_id', $run_id);

        if ($existing_hash === $hash) {
            $this->increment_run_counter($run_id, 'total_processed', 1);
            return;
        }

        $settings = $this->get_settings();
        if ($settings['update_behavior'] === 'full' || $is_new) {
            wp_update_post([
                'ID' => $existing_id,
                'post_title' => $data['title'],
                'post_content' => $this->compose_content($data['description'], $data['features']),
                'post_excerpt' => wp_trim_words(wp_strip_all_tags($data['description']), 40),
            ]);
        }

        foreach ($data['meta'] as $k => $v) {
            update_post_meta($existing_id, $k, $v);
        }

        update_post_meta($existing_id, '_apits_hash', $hash);

        $image_ids = $this->import_images($existing_id, $data['images']);
        update_post_meta($existing_id, '_apits_image_ids', wp_json_encode($image_ids));

        if (! empty($image_ids)) {
            set_post_thumbnail($existing_id, $image_ids[0]);
            if ($settings['update_behavior'] === 'full' || $is_new) {
                $gallery = '[gallery ids="' . implode(',', array_map('intval', $image_ids)) . '"]';
                wp_update_post([
                    'ID' => $existing_id,
                    'post_content' => $this->compose_content($data['description'], $data['features']) . "\n\n" . $gallery,
                ]);
            }
        }

        $this->assign_taxonomies($existing_id, $data);
        $this->write_acf_aliases($existing_id, $data['meta']);

        $this->increment_run_counter($run_id, 'total_processed', 1);
        $this->increment_run_counter($run_id, $is_new ? 'total_created' : 'total_updated', 1);
    }

    private function parse_listing($html, $url)
    {
        $data = [
            'title' => '',
            'description' => '',
            'features' => [],
            'images' => [],
            'meta' => [
                '_apits_source_url' => $url,
                '_apits_listing_ref' => '',
                '_apits_agent_id' => $this->extract_agent_id($this->get_settings()['portfolio_url']),
                '_apits_price_gbp' => '',
                '_apits_price_alt' => '',
                '_apits_beds' => '',
                '_apits_baths' => '',
                '_apits_location_country' => '',
                '_apits_location_region' => '',
                '_apits_location_city' => '',
            ],
        ];

        libxml_use_internal_errors(true);
        $dom = new DOMDocument();
        $dom->loadHTML($html);
        libxml_clear_errors();
        $xpath = new DOMXPath($dom);

        $h1 = $xpath->query('//h1')->item(0);
        if ($h1) {
            $data['title'] = trim($h1->textContent);
        }

        if (preg_match('/\(Ref:\s*([A-Z0-9]+)\)/i', $html, $m)) {
            $data['meta']['_apits_listing_ref'] = strtoupper(trim($m[1]));
        } elseif (preg_match('~/property/details/([a-zA-Z0-9]+)~', $url, $m)) {
            $data['meta']['_apits_listing_ref'] = strtoupper($m[1]);
        }

        $descNode = $xpath->query('//div[contains(@class,"description") or contains(@class,"property-description") or @id="description"]')->item(0);
        if ($descNode) {
            $data['description'] = trim(wp_strip_all_tags($dom->saveHTML($descNode), true));
        }
        if (! $data['description']) {
            $paragraphs = $xpath->query('//main//p');
            $buffer = [];
            foreach ($paragraphs as $p) {
                $text = trim($p->textContent);
                if (strlen($text) > 40) {
                    $buffer[] = $text;
                }
            }
            $data['description'] = implode("\n\n", array_slice($buffer, 0, 8));
        }

        $featureNodes = $xpath->query('//ul[contains(@class,"feature") or contains(@class,"features")]//li');
        foreach ($featureNodes as $featureNode) {
            $feature = trim(wp_strip_all_tags($featureNode->textContent));
            if ($feature) {
                $data['features'][] = $feature;
            }
        }
        $data['features'] = array_values(array_unique($data['features']));

        $breadcrumbLinks = $xpath->query('//nav[contains(@class,"breadcrumb") or @aria-label="breadcrumb"]//a');
        $locations = [];
        foreach ($breadcrumbLinks as $link) {
            $text = trim($link->textContent);
            if ($text) {
                $locations[] = $text;
            }
        }
        $locations = array_values(array_filter($locations));
        if (! empty($locations)) {
            $tail = array_slice($locations, -3);
            $data['meta']['_apits_location_country'] = $tail[0] ?? '';
            $data['meta']['_apits_location_region'] = $tail[1] ?? '';
            $data['meta']['_apits_location_city'] = $tail[2] ?? '';
        }

        if (preg_match('/£\s?([0-9,]+)/', $html, $m)) {
            $data['meta']['_apits_price_gbp'] = (int) str_replace(',', '', $m[1]);
        }
        if (preg_match('/\[(.*?)\]/', $html, $m)) {
            $data['meta']['_apits_price_alt'] = sanitize_text_field($m[1]);
        }
        if (preg_match('/(\d+)\s*beds?/i', $html, $m)) {
            $data['meta']['_apits_beds'] = (int) $m[1];
        }
        if (preg_match('/(\d+)\s*baths?/i', $html, $m)) {
            $data['meta']['_apits_baths'] = (int) $m[1];
        }

        $data['images'] = $this->extract_images($xpath, $html, $url);

        if (! $data['title']) {
            $data['title'] = $data['meta']['_apits_listing_ref'] ?: 'APITS Property';
        }

        return $data;
    }

    private function extract_images(DOMXPath $xpath, $html, $url)
    {
        $images = [];

        $jsonNodes = $xpath->query('//script[@type="application/ld+json"]');
        foreach ($jsonNodes as $node) {
            $json = json_decode($node->textContent, true);
            if (! $json) {
                continue;
            }
            $images = array_merge($images, $this->extract_images_from_json($json));
        }

        if (empty($images)) {
            $imgNodes = $xpath->query('//img[@src]');
            foreach ($imgNodes as $imgNode) {
                $src = trim($imgNode->getAttribute('src'));
                if (! $src) {
                    continue;
                }
                if (strpos($src, 'data:image') === 0) {
                    continue;
                }
                if (strpos($src, 'placeholder') !== false) {
                    continue;
                }
                $images[] = $this->normalize_url($src, $url);
            }
        }

        if (preg_match_all('~https?://[^\"\']+\.(?:jpg|jpeg|png|webp)~i', $html, $m)) {
            $images = array_merge($images, $m[0]);
        }

        $normalized = [];
        foreach ($images as $image) {
            $image = esc_url_raw($image);
            if (! $image) {
                continue;
            }
            $image = preg_replace('/\?.*$/', '', $image);
            if ($image) {
                $normalized[] = $image;
            }
        }

        return array_values(array_unique($normalized));
    }

    private function extract_images_from_json($json)
    {
        $images = [];

        if (is_array($json)) {
            foreach ($json as $key => $value) {
                if ($key === 'image') {
                    if (is_string($value)) {
                        $images[] = $value;
                    } elseif (is_array($value)) {
                        foreach ($value as $v) {
                            if (is_string($v)) {
                                $images[] = $v;
                            }
                        }
                    }
                }

                if (is_array($value)) {
                    $images = array_merge($images, $this->extract_images_from_json($value));
                }
            }
        }

        return $images;
    }

    private function compose_content($description, $features)
    {
        $content = trim($description);

        if (! empty($features)) {
            $content .= "\n\n" . "<h3>Features</h3>\n<ul>";
            foreach ($features as $feature) {
                $content .= '<li>' . esc_html($feature) . '</li>';
            }
            $content .= '</ul>';
        }

        return $content;
    }

    private function import_images($post_id, $image_urls)
    {
        if (empty($image_urls)) {
            return [];
        }

        require_once ABSPATH . 'wp-admin/includes/file.php';
        require_once ABSPATH . 'wp-admin/includes/media.php';
        require_once ABSPATH . 'wp-admin/includes/image.php';

        $existing = json_decode((string) get_post_meta($post_id, '_apits_image_ids', true), true);
        $existing = is_array($existing) ? array_map('intval', $existing) : [];

        $ids = [];

        foreach ($image_urls as $url) {
            $existing_id = $this->find_attachment_by_source_url($url);
            if ($existing_id) {
                $ids[] = $existing_id;
                continue;
            }

            $attachment_id = media_sideload_image($url, $post_id, null, 'id');
            if (is_wp_error($attachment_id)) {
                continue;
            }

            update_post_meta($attachment_id, '_apits_source_image_url', esc_url_raw($url));
            $ids[] = (int) $attachment_id;
        }

        if (empty($ids) && ! empty($existing)) {
            return $existing;
        }

        return array_values(array_unique(array_filter(array_map('intval', $ids))));
    }

    private function find_attachment_by_source_url($url)
    {
        global $wpdb;

        return (int) $wpdb->get_var($wpdb->prepare(
            "SELECT post_id FROM {$wpdb->postmeta} WHERE meta_key = '_apits_source_image_url' AND meta_value = %s LIMIT 1",
            $url
        ));
    }

    private function assign_taxonomies($post_id, $data)
    {
        $country = $data['meta']['_apits_location_country'];
        $region = $data['meta']['_apits_location_region'];
        $city = $data['meta']['_apits_location_city'];

        if ($country) {
            wp_set_post_terms($post_id, [$country], 'apits_country', false);
        }
        if ($region) {
            wp_set_post_terms($post_id, [$region], 'apits_region', false);
        }
        if ($city) {
            wp_set_post_terms($post_id, [$city], 'apits_city', false);
        }

        if (preg_match('/\b(apartment|villa|duplex|condo|townhouse|studio)\b/i', $data['title'], $m)) {
            wp_set_post_terms($post_id, [ucfirst(strtolower($m[1]))], 'apits_property_type', false);
        }
    }

    private function write_acf_aliases($post_id, $meta)
    {
        if (! $this->get_settings()['acf_map']) {
            return;
        }

        $aliases = [
            '_apits_price_gbp' => 'apits_price_gbp',
            '_apits_price_alt' => 'apits_price_alt',
            '_apits_beds' => 'apits_beds',
            '_apits_baths' => 'apits_baths',
            '_apits_source_url' => 'apits_source_url',
            '_apits_listing_ref' => 'apits_listing_ref',
            '_apits_location_country' => 'apits_country',
            '_apits_location_region' => 'apits_region',
            '_apits_location_city' => 'apits_city',
        ];

        foreach ($aliases as $src => $dest) {
            if (array_key_exists($src, $meta)) {
                update_post_meta($post_id, $dest, $meta[$src]);
            }
        }
    }

    private function find_existing_post_id($source_url, $listing_ref)
    {
        global $wpdb;

        $post_id = (int) $wpdb->get_var($wpdb->prepare(
            "SELECT p.ID FROM {$wpdb->posts} p INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id WHERE p.post_type = %s AND pm.meta_key = '_apits_source_url' AND pm.meta_value = %s LIMIT 1",
            self::CPT,
            $source_url
        ));

        if ($post_id) {
            return $post_id;
        }

        if (! $listing_ref) {
            return 0;
        }

        return (int) $wpdb->get_var($wpdb->prepare(
            "SELECT p.ID FROM {$wpdb->posts} p INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id WHERE p.post_type = %s AND pm.meta_key = '_apits_listing_ref' AND pm.meta_value = %s LIMIT 1",
            self::CPT,
            $listing_ref
        ));
    }

    private function run_finalize_job($run_id)
    {
        global $wpdb;
        $jobs_table = $wpdb->prefix . self::JOBS_TABLE;
        $runs_table = $wpdb->prefix . self::RUNS_TABLE;

        $pending_count = (int) $wpdb->get_var($wpdb->prepare(
            "SELECT COUNT(1) FROM {$jobs_table} WHERE run_id = %d AND job_type = %s AND status IN ('queued','running','retrying')",
            $run_id,
            self::JOB_IMPORT_LISTING
        ));

        if ($pending_count > 0) {
            $this->enqueue_job($run_id, self::JOB_FINALIZE, [], 60);
            return;
        }

        $run = $wpdb->get_row($wpdb->prepare("SELECT * FROM {$runs_table} WHERE id = %d", $run_id), ARRAY_A);
        if (! $run) {
            throw new Exception('Run not found in finalize');
        }

        $agent_id = $this->extract_agent_id($this->get_settings()['portfolio_url']);

        $args = [
            'post_type' => self::CPT,
            'post_status' => ['publish', 'draft', 'archived'],
            'posts_per_page' => -1,
            'fields' => 'ids',
            'meta_query' => [
                ['key' => '_apits_agent_id', 'value' => $agent_id],
                ['key' => '_apits_last_seen_at', 'value' => $run['started_at'], 'compare' => '<', 'type' => 'DATETIME'],
            ],
        ];

        $posts = get_posts($args);
        $archive_status = $this->get_settings()['archive_behavior'] === 'archived' ? 'archived' : 'draft';

        foreach ($posts as $post_id) {
            wp_update_post(['ID' => $post_id, 'post_status' => $archive_status]);
        }

        $final_status = ((int) $run['total_failed'] > 0) ? 'partial' : 'success';

        $wpdb->update($runs_table, [
            'status' => $final_status,
            'finished_at' => current_time('mysql'),
            'total_archived' => count($posts),
        ], ['id' => $run_id]);
    }

    private function extract_agent_id($portfolio_url)
    {
        if (preg_match('~/property/agent/(\d+)/~', $portfolio_url, $m)) {
            return $m[1];
        }

        return '';
    }

    private function increment_run_counter($run_id, $column, $by)
    {
        global $wpdb;
        $runs_table = $wpdb->prefix . self::RUNS_TABLE;

        $allowed = ['total_processed', 'total_created', 'total_updated', 'total_failed', 'total_archived'];
        if (! in_array($column, $allowed, true)) {
            return;
        }

        $wpdb->query($wpdb->prepare(
            "UPDATE {$runs_table} SET {$column} = {$column} + %d WHERE id = %d",
            (int) $by,
            (int) $run_id
        ));
    }

    private function create_tables()
    {
        global $wpdb;

        require_once ABSPATH . 'wp-admin/includes/upgrade.php';

        $charset_collate = $wpdb->get_charset_collate();
        $runs_table = $wpdb->prefix . self::RUNS_TABLE;
        $jobs_table = $wpdb->prefix . self::JOBS_TABLE;

        $sql_runs = "CREATE TABLE {$runs_table} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            started_at DATETIME NOT NULL,
            finished_at DATETIME NULL,
            trigger_type VARCHAR(16) NOT NULL DEFAULT 'manual',
            status VARCHAR(16) NOT NULL DEFAULT 'running',
            total_discovered INT NOT NULL DEFAULT 0,
            total_processed INT NOT NULL DEFAULT 0,
            total_created INT NOT NULL DEFAULT 0,
            total_updated INT NOT NULL DEFAULT 0,
            total_failed INT NOT NULL DEFAULT 0,
            total_archived INT NOT NULL DEFAULT 0,
            last_error TEXT NULL,
            PRIMARY KEY  (id)
        ) {$charset_collate};";

        $sql_jobs = "CREATE TABLE {$jobs_table} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            run_id BIGINT UNSIGNED NOT NULL,
            job_type VARCHAR(32) NOT NULL,
            payload_json LONGTEXT NULL,
            status VARCHAR(16) NOT NULL DEFAULT 'queued',
            attempts INT NOT NULL DEFAULT 0,
            last_error TEXT NULL,
            locked_until DATETIME NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY  (id),
            KEY run_id (run_id),
            KEY status_locked_until (status, locked_until)
        ) {$charset_collate};";

        dbDelta($sql_runs);
        dbDelta($sql_jobs);
    }

    private function register_cli()
    {
        WP_CLI::add_command('apits-mortify import', function () {
            $this->start_run('cli');
            WP_CLI::success('APITS import run queued.');
        });
    }
}
