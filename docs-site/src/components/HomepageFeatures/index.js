import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';
import {
  Shield,
  RefreshCw,
  AlertCircle,
  ArrowRight,
  Tag,
  Camera,
  Search,
  Building,
  Puzzle,
} from 'lucide-react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

const FeatureList = [
  {
    title: 'ACID Compliance',
    icon: Shield,
    description: (
      <>Fully ACID-compliant event storage for reliable, consistent operations.</>
    ),
  },
  {
    title: 'Optimistic Concurrency',
    icon: RefreshCw,
    description: (
      <>Built-in optimistic concurrency control to prevent conflicts and ensure data integrity.</>
    ),
  },
  {
    title: 'Duplicate Event Detection',
    icon: AlertCircle,
    description: (
      <>Automatic detection and prevention of duplicate events based on identity.</>
    ),
  },
  {
    title: 'Automatic Continuation',
    icon: ArrowRight,
    description: (
      <>Seamless handling of Azure Table Storage batch limits for both reads and writes.</>
    ),
  },
  {
    title: 'Custom Properties',
    icon: Tag,
    description: (
      <>Support for custom stream and event properties that you can query on.</>
    ),
  },
  {
    title: 'Inline Projections & Snapshots',
    icon: Camera,
    description: (
      <>Synchronous projections and snapshots for fast, up-to-date views.</>
    ),
  },
  {
    title: 'Change Tracking',
    icon: Search,
    description: (
      <>Change tracking support for inline projections and state management.</>
    ),
  },
  {
    title: 'Multi-Tenant Friendly',
    icon: Building,
    description: (
      <>Designed for multi-tenant architectures and scalable SaaS solutions.</>
    ),
  },
  {
    title: 'Sharding Support',
    icon: Puzzle,
    description: (
      <>Jump consistent hashing for efficient sharding and horizontal scalability.</>
    ),
  },
];

function Feature({icon: Icon, title, description}) {
  // Use CSS variable for primary color
  const primaryColor = typeof window !== 'undefined'
    ? getComputedStyle(document.documentElement).getPropertyValue('--ifm-color-primary') || '#2992E1'
    : '#2992E1';
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center" style={{marginBottom: 12}}>
        <Icon size={48} strokeWidth={1.5} color={primaryColor.trim()} />
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
} 